
build-wasi-alloy:
	cd src/alloy && cargo build --target=wasm32-wasip1 --release && wasm-opt ../../target/wasm32-wasip1/release/alloy.wasm -o ../../target/wasm32-wasip1/release/alloy_optimized.wasm -O2
	cp target/wasm32-wasip1/release/alloy_optimized.wasm xp/alloy.wasm

build-wasi-alloy-nolog:
	cd src/alloy && cargo build --target=wasm32-wasip1 --release --features release_max_level_warn && wasm-opt ../../target/wasm32-wasip1/release/alloy.wasm -o ../../target/wasm32-wasip1/release/alloy_optimized.wasm -O2
	cp target/wasm32-wasip1/release/alloy_optimized.wasm xp/alloy.wasm

unit-test: # not working because of https://github.com/rustwasm/wasm-bindgen/issues/2594
	cd src/routing && cargo test -- --nocapture

push-alloy-nolog: build-wasi-alloy-nolog
	docker build . -t grosinosky/envoy:v1.29-alloy-nolog
	docker push grosinosky/envoy:v1.29-alloy-nolog

push-alloy-nolog_test: build-wasi-alloy-nolog
	docker build . -t grosinosky/envoy:v1.29-alloy-nolog_test
	docker push grosinosky/envoy:v1.29-alloy-nolog_test

push-alloy: build-wasi-alloy
	docker build . -t grosinosky/envoy:v1.29-alloy
	docker push grosinosky/envoy:v1.29-alloy

push-alloy_test: build-wasi-alloy
	docker build . -t grosinosky/envoy:v1.29-alloy_test
	docker push grosinosky/envoy:v1.29-alloy_test
