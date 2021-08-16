docker.run:
	docker-compose up -d
test:
	go test ./... -v -race
