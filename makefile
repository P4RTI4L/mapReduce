default:
	gcc -g -std=gnu99 -pthread -o project -Wall -Wextra -Wno-unused-parameter mapReduce.c mapReduce.h
clean:
	rm project
