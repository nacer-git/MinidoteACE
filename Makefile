REBAR = ./rebar3

DEPS_PATH = $(shell $(REBAR) as test path)


CT_OPTS_DIST = -erl_args

ifdef SUITE
CT_OPTS_SUITE = -suite test/${SUITE}
else
CT_OPTS_SUITE =
endif

all: compile

compile:
	$(REBAR) compile

clean:
	rm -rf ebin/* test/*.beam logs log
	$(REBAR) clean

distclean: clean
	rm -rf _build priv/*.so logs log

dialyzer:
	$(REBAR) dialyzer

shell:
	$(REBAR) shell

compile_tests:
	$(REBAR) as test compile


ifeq ($(OS),Windows_NT) 
#windows
test: compile_tests
	-mkdir logs
	ct_run -dir test -pa $(DEPS_PATH) -logdir logs ${CT_OPTS_SUITE} $(CT_OPTS_DIST)
else 
#normal operating system
test: compile_tests
	mkdir -p logs
	ct_run -dir test -pa $(DEPS_PATH) -logdir logs ${CT_OPTS_SUITE} $(CT_OPTS_DIST) || (rm -rf test/*.beam && false)
	rm -rf test/*.beam
endif

rel:
	rm -rf _build/default/rel/
	$(REBAR) release

docker:
	docker build -f Dockerfiles/Dockerfile -t minidote .

docker-run:
	docker run --rm -it -p 8087:8087 minidote

docker-run-cluster:
	docker-compose up

docker-build-tcb:
	docker build -f Dockerfiles/Dockerfile-tcb -t gyounes/minidote:latest .

docker-run-tcb:
	docker run -d --name minidote -p 8087:8087 gyounes/minidote:latest

run-cluster-node1:
	MINIDOTE_NODES=minidote-10017@localhost,minidote-10018@localhost LOG_DIR=./data/m1/ OP_LOG_DIR=./data/m1/op_log/ MINIDOTE_PORT=8096 MEMBERS=minidote-10017@localhost,minidote-10018@localhost CAMUS_PORT=10016 ./rebar3 shell --name minidote-10016@localhost
run-cluster-node2:
	MINIDOTE_NODES=minidote-10016@localhost,minidote-10018@localhost LOG_DIR=./data/m2/ OP_LOG_DIR=./data/m2/op_log/ MINIDOTE_PORT=8097 MEMBERS=minidote-10016@localhost,minidote-10018@localhost CAMUS_PORT=10017 ./rebar3 shell --name minidote-10017@localhost 
run-cluster-node3:
	MINIDOTE_NODES=minidote-10016@localhost,minidote-10017@localhost LOG_DIR=./data/m3/ OP_LOG_DIR=./data/m3/op_log/ MINIDOTE_PORT=8098 MEMBERS=minidote-10016@localhost,minidote-10017@localhost CAMUS_PORT=10018 ./rebar3 shell --name minidote-10018@localhost 

tcb:
	pkill -9 beam.smp ; rm -rf priv/lager ; ${REBAR} ct --readable=false --verbose --suite minidote_tcb_remote_tests_SUITE
