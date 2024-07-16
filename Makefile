include .env
export $(shell sed 's/=.*//' .env)

build:
	cargo build --release

run:
	cargo run
run-release:
	./target/release/feats_of_distance