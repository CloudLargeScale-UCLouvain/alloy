import json
import re
from collections import defaultdict
from typing_extensions import Self
import json_fix
import yaml
from typing import List

class Operator:
    """
    A class used to manipulate a Flink operators

    Parameters
    ----------
    operator_id : int
        the operator id
    desc : dict
        the operator description

    Attributes
    ----------
    pred : List[Operator]
        the list of upstream operators
    succ : List[Operator]
        the list of downstream operators
    """
    def __init__(self, operator_id: int, desc: dict):
        self.operator_id: int = operator_id
        self.desc: dict = desc
        self.pred = []
        self.succ = []

    def add_pred(self, pred: Self):
        self.pred += [pred]

    def add_succ(self, succ: Self):
        self.succ += [succ]

    def __json__(self):
        return f"Operator [{self.operator_id}]"

def extract_source_table(operator: Operator) -> str:
    return operator.desc["scanTableSource"]["table"]["identifier"].split('.')[-1]

class Graph:
    """
    A class used to manipulate a graph of operators

    Parameters
    ----------
    plan : dict
        the original plan as given by Flink

    Attributes
    ----------
    operators : List[Operator]
        a list of operators extracted from the plan
    sources : List[Operator]
        the list of source operators
    sinks : List[Operator]
        the list of sink operators
    """

    def __init__(self, plan: dict):
        def create_graph(plan: dict):
            operators = {}
            for node in plan["nodes"]:
                operators[node["id"]] = Operator(node["id"], node)

            for edge in plan["edges"]:
                operators[edge["target"]].add_pred(operators[edge["source"]])
                operators[edge["source"]].add_succ(operators[edge["target"]])
            return operators

        self.plan: dict = plan
        self.operators: dict = create_graph(plan)
        self.sources = []
        self.sinks = []

        for o in self.operators.values():
            if "scanTableSource" in o.desc.keys():
                self.sources += [o]
            elif "dynamicTableSink" in o.desc.keys():
                self.sinks += [o]


    def get_sinks_fields(self):
        if len(self.sinks) != 1:
            return []
        return [c["name"] for c in self.sinks[0].desc["dynamicTableSink"]["table"]["resolvedTable"]["schema"]["columns"]]

    def get_sources_fields(self):
        if len(self.sources) < 1:
            return []
        fields = {}
        for s in self.sources:
            fields[extract_source_table(s)]= [c["name"] for c in s.desc["scanTableSource"]["table"]["resolvedTable"]["schema"]["columns"]]
        return fields

    def track_fields_projection(self, succ: Operator, fields):
        def track_embedded_operands(projection: dict) -> int:
            for o in projection["operands"]:
                if "inputIndex" in o:
                    return o["inputIndex"]
                elif "operands" in o:
                    return track_embedded_operands(o)

        projector_node = None
        new_fields = []
        if "projection" in succ.desc and succ.desc["projection"]:
            projector_node = succ
            for p in succ.desc["projection"]:
                if "inputIndex" in p:
                    new_fields += [fields[p["inputIndex"]]]
                else:
                    index = track_embedded_operands(p)
                    if index:
                        new_fields += [fields[index]]
        return (new_fields, projector_node)

    def get_projections(self):
        fields = {}
        for s in self.sources:
            if len(s.succ) == 1:
                print(s.operator_id)
                fields[extract_source_table(s)] = (self.track_fields_projection(s.succ[0], [c["name"] for c in s.desc["scanTableSource"]["table"]["resolvedTable"]["schema"]["columns"]]), s)
        return fields

    def get_selections(self):
        def backtrack_field(inputIndex: int, pred) -> str:
            for p in pred:
                if "outputType" in p.desc.keys():
                    return p.desc["scanTableSource"]["table"]["resolvedTable"]["schema"]["columns"][inputIndex]["name"]
                else:
                    return backtrack_field(inputIndex, p.pred)

        def extract_binary_operands(operands: dict, pred) -> str:
            if not "operands" in operands.keys():
                if "value" in operands.keys():
                    return str(operands["value"])
                elif "inputIndex" in operands.keys() :
                    return str(backtrack_field(operands["inputIndex"], pred))
                else:
                    return ""

            left = extract_binary_operands(operands["operands"][0], pred)
            right = extract_binary_operands(operands["operands"][1], pred)
            condition_operator = operands["internalName"]
            condition_operator = str.replace(condition_operator, "$1", " " + right)
            condition_operator = str.replace(condition_operator, "$", left + " ")
            return condition_operator

        def extraxt_search_operands(operands: dict, pred) -> str:
            ranges = [str(x["lower"]["value"]) for x in operands["operands"][1]["sarg"]["ranges"]]
            return backtrack_field(operands["operands"][0]["inputIndex"], pred) + " in " + "|".join(ranges)

        def extract_operands(condition: dict, pred) -> str:
            if condition["syntax"] == "BINARY":
                return extract_binary_operands(condition, pred)
            elif condition["syntax"] == "INTERNAL" and "SEARCH" in condition["internalName"]:
                return extraxt_search_operands(condition, pred)

        def extract_source(operator: Operator) -> str:
            if "scanTableSource" in operator.desc:
                return extract_source_table(operator)
            else:
                return extract_source(operator.pred[0])

        ret = {}
        for s in self.sources:
            if len(s.succ) == 1:
                operator = s.succ[0]
                if "condition" in operator.desc.keys() and operator.desc["condition"]:
                    condition: dict = operator.desc["condition"]
                    selection: str = extract_operands(condition, operator.pred)
                    ret[extract_source(operator)] = (selection, operator)
        return ret

    def get_partitions(self):
        def find_partitionner(operator: Operator) :
            fields = []
            if isinstance(operator.desc["outputType"], str):
                fields = re.findall('`([^`]*)`', operator.desc["outputType"])
                types = re.findall(r'`[^`]+`\s+(\w+)', operator.desc["outputType"])
            else:
                fields = [c["name"] for c in operator.desc["outputType"]["fields"]]
                types = [c["fieldType"] for c in operator.desc["outputType"]["fields"]]
            if operator.desc["type"] == "stream-exec-exchange_1":
                return ([{"attribute": fields[i], "field_type": types[i] } for i in operator.desc["inputProperties"][0]["requiredDistribution"]["keys"]], operator)
            else:
                for succ in operator.succ:
                    return find_partitionner(succ)

        partition = {}
        for s in self.sources:
            partition[extract_source_table(s)] = find_partitionner(s)
        return partition

    def get_alloy_plan(self) -> dict:
        selections = self.get_selections()
        projections = self.get_projections()
        partitions = self.get_partitions()

        ret = defaultdict(dict)
        for s, v in selections.items():
            ret[s]["selections"] = v
        for s, v in projections.items():
            ret[s]["projections"] = v
        for s, v in partitions.items():
            ret[s]["partitions"] = v

        return ret

    def apply_alloy_plan(self, alloy_plan=None, envoy:str="envoy1:9093", apply_projection:bool=True, apply_selection:bool=True, apply_partition:bool=True) -> dict:

        def bypass_node(new_plan: dict, operator_id: int):
            def find_target_node(edges: list, sid: int):
                return next(e["target"] for e in edges if e["source"] == sid)

            def delete_edge(new_plan: dict, operator_id: int):
                edges = []
                nodes = []
                for e in new_plan["edges"]:
                    if not e["target"] == operator_id and not e["source"] == operator_id:
                        edges += [e]
                for n in new_plan["nodes"]:
                    if not n["id"] == operator_id:
                        nodes += [n]
                new_plan["edges"] = edges
                new_plan["nodes"] = nodes


            for e in new_plan["edges"]:
                if e["target"] == operator_id:
                    e["target"] = find_target_node(new_plan["edges"], operator_id)
                    delete_edge(new_plan, operator_id)

        def apply_projection(new_plan: dict, alloy_plan):
            def remove_columns(node, fields) :
                """ Returns the mapping between the old field indexes and the new ones, to be propagated
                    to the upstream operator(s)
                """
                columns = []
                new_input_indexes = {}
                index = 0
                new_index = 0
                for c in node["scanTableSource"]["table"]["resolvedTable"]["schema"]["columns"]:
                    if c["name"] in fields:
                        columns += [c]
                        new_input_indexes[index] = new_index
                        new_index += 1
                    index += 1
                node["scanTableSource"]["table"]["resolvedTable"]["schema"]["columns"] = columns
                node["outputType"] = {"type" : "ROW", "fields": [{"name": cc["name"], "fieldType": cc["dataType"]} for cc in columns]}
                return new_input_indexes

            def change_next_operator_projection_indexes(nodes, source: Operator, index_mapping):
                def replace_input_index(proj, index_mapping):
                    if "inputIndex" in proj:
                        proj["inputIndex"] = index_mapping[proj["inputIndex"]]
                    else:
                        [replace_input_index(o, index_mapping) for o in proj.get("operands", [])]

                for n in nodes:
                    if n["id"] == source.succ[0].operator_id:
                        for p in n.get("projection", []):
                            replace_input_index(p, index_mapping)

            def remove_watermark_spec(node, fields, index_mapping):
                watermarks = []
                removed = []
                for w in node["scanTableSource"]["table"]["resolvedTable"]["schema"]["watermarkSpecs"]:
                    if w["rowtimeAttribute"] in fields:
                        w["expression"]["rexNode"]["operands"][0]["inputIndex"] = index_mapping[w["expression"]["rexNode"]["operands"][0]["inputIndex"]]
                        watermarks += [w]
                    else:
                        removed += [w["expression"]["rexNode"]["operands"][0]["inputIndex"]]
                node["scanTableSource"]["table"]["resolvedTable"]["schema"]["watermarkSpecs"] = watermarks
                return removed

            def remove_watermark_ability(node, fields, removed, index_mapping):
                if not "abilities" in node["scanTableSource"]:
                    return
                abilities = []
                for a in node["scanTableSource"]["abilities"]:
                    if not a["watermarkExpr"]["operands"][0]["inputIndex"] in removed:
                        a["watermarkExpr"]["operands"][0]["inputIndex"] = index_mapping[a["watermarkExpr"]["operands"][0]["inputIndex"]]
                        new_fields = []
                        for f in a["producedType"]["fields"]:
                            if f["name"] in fields:
                                new_fields += [f]
                            a["producedType"]["fields"] = new_fields
                        abilities += [a]
                if len(abilities) == 0:
                    node["scanTableSource"].pop("abilities", None)
                else:
                    node["scanTableSource"]["abilities"] = abilities

            def change_output_type(node, fields):
                new_fields = []
                for f in node["scanTableSource"]["table"]["resolvedTable"]["schema"]["columns"]:
                    if f["name"] in fields:
                        new_fields += [{"name": f["name"], "fieldType": f["dataType"]}]
                node["outputType"] = {"type": "ROW", "fields": new_fields}

            def has_output_order_changed(node):
                prev_index = -1
                for field in node["projection"]:
                    if field["inputIndex"] < prev_index:
                        return True
                    prev_index = field["inputIndex"]
                return False
            
            def new_projection_mapping(node):
                return {i: field["inputIndex"] for i, field in enumerate(node["projection"])}
            
            def reorder_source_fields(new_plan, node: Operator, new_mapping):
                fields = []
                schemas = [] 
                for op in new_plan["nodes"]:
                    if op["id"] == node.operator_id:
                        for _,v in new_mapping.items():
                            field = op["outputType"]["fields"][v]
                            fields += [{"name": field["name"], "fieldType": field["fieldType"]}]
                            schemas += [{"name": field["name"], "dataType": field["fieldType"]}]
                        op["outputType"]["fields"] = fields
                        op["scanTableSource"]["table"]["resolvedTable"]["schema"]["columns"] = schemas

                        if "watermarkSpecs" in op["scanTableSource"]["table"]["resolvedTable"]["schema"]:
                            attribute = op["scanTableSource"]["table"]["resolvedTable"]["schema"]["watermarkSpecs"][0]["rowtimeAttribute"]
                            index = next(i for i, field in enumerate(schemas) if field["name"] == attribute)
                            op["scanTableSource"]["table"]["resolvedTable"]["schema"]["watermarkSpecs"][0]["expression"]["rexNode"]["operands"][0]["inputIndex"] = index
                            op["scanTableSource"]["abilities"][0]["watermarkExpr"]["operands"][0]["inputIndex"] = index
                        return
                    
            def check_can_bypass_projection_node(node: Operator):
                #display(self.operators[node["id"]].pred[0].desc)
                if node["condition"]:
                    return False
                #if has_output_order_changed(node):
                #    return False
                if len(node["projection"]) != len(self.operators[node["id"]].pred[0].desc["outputType"]["fields"]):
                    return False
                for p in node["projection"]:
                    if p["kind"] == "CALL":
                        return False
                return True

            source = alloy_plan[1]
            fields = alloy_plan[0][0]
            for n in new_plan["nodes"]:
                if n["id"] == source.operator_id:
                    new_input_indexes = remove_columns(n, fields)
                    change_next_operator_projection_indexes(new_plan["nodes"], source, new_input_indexes)
                    removed = remove_watermark_spec(n, fields, new_input_indexes)
                    remove_watermark_ability(n, fields, removed, new_input_indexes)
                    change_output_type(n, fields)
                    #change_topic_name(n)
                elif "calc" in n["type"] and "source" in self.operators[n["id"]].pred[0].desc["type"]:
                    if check_can_bypass_projection_node(n):
                        if has_output_order_changed(n):
                            new_input_indexes = new_projection_mapping(n)
                            reorder_source_fields(new_plan, self.operators[n["id"]].pred[0], new_input_indexes)
                        bypass_node(new_plan, n["id"])

        def apply_selections(new_plan: dict, alloy_plan):
            for n in new_plan["nodes"]:
                if n["id"] == alloy_plan[1].operator_id:
                    can_be_removed = True
                    for p in n.get("projection", []):
                        if not "inputIndex" in p:
                            can_be_removed = False
                    if can_be_removed:
                        bypass_node(new_plan, alloy_plan[1].operator_id)
                    else:
                        n["condition"] = None


        def apply_partitions(new_plan: dict, alloy_plan):
            bypass_node(new_plan, alloy_plan[1].operator_id)

        def assert_output_type_is_dict(node: Operator):
            if isinstance(node["outputType"], str):
                columns = []
                for field in node["scanTableSource"]["table"]["resolvedTable"]["schema"]["columns"]:
                    columns += [{"name": field["name"], "fieldType": field["dataType"]}]
                node["outputType"] = {"type": "ROW", "fields": columns}

        def replace_kafka_bootstrap_server(node: Operator, envoy: str):
            node["scanTableSource"]["table"]["resolvedTable"]["options"]["properties.bootstrap.servers"] = envoy

        def replace_catalog_name(node: Operator):
            node["scanTableSource"]["table"]["identifier"] = node["scanTableSource"]["table"]["identifier"].replace("default", "alloy")

        if not alloy_plan:
            alloy_plan = self.get_alloy_plan()
        new_plan = self.plan.copy()

        for node in new_plan["nodes"]:
            if "source" in node["type"]:
                assert_output_type_is_dict(node)
                if envoy != "":
                    replace_kafka_bootstrap_server(node, envoy)
                replace_catalog_name(node)


        for v in alloy_plan.values():
            selections = v.get("selections", None)
            projections = v.get("projections", None)
            partitions = v.get("partitions", None)
            if partitions and apply_partition:
                apply_partitions(new_plan, partitions)
            if selections and apply_selection:
                apply_selections(new_plan, selections)
            if projections and apply_projection:
                apply_projection(new_plan, projections)

        return json.dumps(new_plan)

def write_alloy_plan(plan, apply_projection:bool=True, apply_selection:bool=True, apply_partition:bool=True):
    def write_selections(selections):
        splitted = selections.split(" in ")
        # TODO: fix the get_selections to return the correct format for IN conditions
        return [{"attribute": splitted[0], "value": splitted[1]}]
    ret = dict()
    for k,v in plan.items():
        topic = k.replace('`','')
        ret[topic] = dict()
        ret[topic]["projections"] = v.get("projections", [[None]])[0][0]
        if ret[topic]["projections"] is None:
            ret[topic]["projections"] = []
        ret[topic]["selections"] = v.get("selections", [None])[0]
        if ret[topic]["selections"] is None:
            ret[topic]["selections"] = []
        else: 
            print("Test")
            ret[topic]["selections"] = write_selections(ret[topic]["selections"])
        if v.get("partitions", None) is not None:
            ret[topic]["partition"] = v.get("partitions", [None])[0]
            if ret[topic]["partition"] is None or not apply_partition:
                ret[topic]["partition"] = []
        else:
            ret[topic]["partition"] = []
    return ret

import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str)
    parser.add_argument("--output_dir", type=str, default="")
    parser.add_argument("--envoy", type=str, default="")
    parser.add_argument("--apply", action="store_true", default=False)
    parser.add_argument("--projection", action="store_true", default=False)
    parser.add_argument("--selection", action="store_true", default=False)
    parser.add_argument("--partition", action="store_true", default=False)
    args = parser.parse_args()

    with open(args.input_path) as f:
        plan = json.load(f)

    graph = Graph(plan)
    alloy_plan = graph.get_alloy_plan()

    if args.apply:
        new_plan = graph.apply_alloy_plan(alloy_plan, args.envoy, args.projection, args.selection, args.partition)
        with open(f"{args.output_dir}/compiled_plan.json", "w+") as f:
            f.write(new_plan)
        print(write_alloy_plan(alloy_plan, args.projection, args.selection, args.partition))
        print(yaml.dump(write_alloy_plan(alloy_plan, args.projection, args.selection, args.partition)))
    else:
        print(alloy_plan)

if __name__ == "__main__":
    main()