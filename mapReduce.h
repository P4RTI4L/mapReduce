#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>
#define Error(Str) fprintf(stderr, "%s\n", Str), exit(1)

struct Map
{
	int replicas;
	int buffers;
	struct List *addrList;	//buffer between adder and reducer
	struct List *rdrList;	//buffer between reader and adder
	int endMap;
	int beginMap;
	int threadno;
	FILE *filePointer;
} ;


typedef struct Node
{
	char *word;
	int count;
	struct Node *next;
} node;	
typedef struct List
{
	node *head;
	node *tail;
	int size;
} list;

void *map_reader();
void *map_adder();
void reducer();
int search_list(list *l, node *n);
void addToList(list *l, node *n);
void removeFromList (list *l);
char* removeSymbols(char *word);
void lowercase(char *input);