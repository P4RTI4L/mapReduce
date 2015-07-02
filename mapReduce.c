#include "mapReduce.h"
int _fileSize;
int _mapBegin;
int _mapEnd;
struct Map *_map;
list *_addList;
list *_readList;
pthread_mutex_t *_myMutex;
char filename[100];

//for report
struct timeval stop, start;


int main (int argc, char *argv[])
{
	//start time for report
	gettimeofday(&start, NULL);
	
	int bufferSize;
	int numReplicas;
	int consume, produce;
   	long t;
	
	if (argc != 3){
		strcpy(filename, argv[1]);
		sscanf(argv[2], "%d", &numReplicas);
		sscanf(argv[3], "%d", &bufferSize);
	}
	else {
		Error("Invalid parameters to program: ./project3 [file] [number of threads] [buffer size]\n");

	}

	_map = calloc(1, sizeof(struct Map)*numReplicas);
	for(int i = 0; i < numReplicas; i++)
	{
		_map[i].replicas = numReplicas;
		_map[i].buffers = bufferSize;
	}

   	FILE *fp = fopen(filename, "r"); 
	//get file size
	fseek(fp, 0L, SEEK_END);
	_fileSize = ftell(fp);
	//reset file pointer
	fseek(fp, 0L, SEEK_SET);
	fclose(fp);
	if (_fileSize % numReplicas != 0)
	{
		_fileSize = _fileSize + (_fileSize % numReplicas);
		printf("File size = %d\n", _fileSize);
	}
   	printf("File size = %d\n", _fileSize);
   	printf("Test file: %s\n", argv[1]);
   	pthread_t readthreads[numReplicas];
	pthread_t addthreads[numReplicas];
	
   	
	_myMutex = calloc(numReplicas, sizeof(pthread_mutex_t));
	

   	for(t=0; t < numReplicas; t++){
      		printf("In main: creating thread %ld\n", t);
		//partition file
		if(t == 0)
		{
			_mapBegin = 0;
		}
		else	{	
			_mapBegin = ((((t)*_fileSize)/numReplicas) + 1);
		}
		_mapEnd = ((t+1)*_fileSize)/numReplicas; 
		_map[t].endMap = _mapEnd;
		_map[t].beginMap = _mapBegin;
	
		//allocate a list for each thread
		_readList = malloc(sizeof(struct List));
		_readList->size = 0;
		_readList->head = NULL;
		_map[t].rdrList = _readList;
		_addList = malloc(sizeof(struct List));
		_addList->size = 0;
		_addList->head = NULL;
		_map[t].addrList = _addList;

		_map[t].threadno = t;

		//init
		pthread_mutex_init(&_myMutex[t], NULL);
		//create threads	
      		produce = pthread_create(&readthreads[t], NULL, map_reader, &(_map[t]));
		consume = pthread_create(&addthreads[t], NULL, map_adder, &(_map[t]));
		
      		if (produce || consume){
         		Error("Error creating pthread");
      		}
   	}
	//join threads
	for(t=0; t < numReplicas; t++)
	{
		printf("joining\n");
		pthread_join(readthreads[t], NULL);
	}
 	
	//reduce
	reducer();
		
	//for report
	gettimeofday(&stop, NULL);
	printf("time: %lu\n", stop.tv_sec - start.tv_sec);
	return 0;
}

void *map_reader(void *arg)
{
	struct Map *readMapper;
	readMapper = (struct Map *)arg;
	char word[100];	
	char *tempword;
	char c;

	readMapper->filePointer = fopen(filename, "r");
	if(readMapper->filePointer == NULL)
	{
		Error("Error opening file\n");
	}
	fseek(readMapper->filePointer, readMapper->beginMap, SEEK_SET);
	if(readMapper->threadno != 0)
		{
			while(c != ' ')
			{
				fseek(readMapper->filePointer, 1L, SEEK_CUR);
				c = (char)fgetc(readMapper->filePointer);
			}
		}
		
	while(ftell(readMapper->filePointer) < readMapper->endMap-1)
	{
		while(readMapper->rdrList->size < readMapper->buffers)
		{
			pthread_mutex_lock(&_myMutex[readMapper->threadno]);		
			fscanf(readMapper->filePointer, "%s", word);
			tempword = removeSymbols(word);
			strcpy(word, tempword);
			lowercase(word);
			//printf("word: %s:\n", word);
			//getchar();
			node *n = malloc(sizeof(node));
			n->word = word;
			n->count = 1;
			//printf("n-word: %s:\n", n->word);
			//getchar();
			addToList(readMapper->rdrList, n);
			//printf("in list word: %s:\n", readMapper->rdrList->tail->word);
			//getchar();
			
			pthread_mutex_unlock(&_myMutex[readMapper->threadno]);
		}
	}		
	
	fclose(readMapper->filePointer);
	
	pthread_exit(NULL);
}

void *map_adder(void *arg)
{
	struct Map *addMapper;
	addMapper = (struct Map *)arg;
	
	while(1)
	{
		while(addMapper->rdrList->size != 0)
		{
			pthread_mutex_lock(&_myMutex[addMapper->threadno]);
			if(search_list(addMapper->addrList, addMapper->rdrList->head) == 1)
			{
				addToList(addMapper->addrList, addMapper->rdrList->head);
				addMapper->addrList->tail->count = 1;
			}
		
			removeFromList(addMapper->rdrList);
			
		pthread_mutex_unlock(&_myMutex[addMapper->threadno]);
		}
	}
	pthread_exit(NULL);
}
void reducer()
{
	
	FILE *ofp = fopen("output.txt", "w");
	if(ofp == NULL)
	{
		Error("Error opening file\n");
	}
	//printf("word: %s:\n", _map[0].addrList->head->word);
				//getchar();

	for(int i = 1; i < _map[0].replicas; i++)
	{
		while(_map[i].addrList->size > 0)
		{
			if(search_list(_map[0].addrList, _map[i].addrList->head) == 1)
			{
				addToList(_map[0].addrList, _map[i].addrList->head);
				
				_map[0].addrList->tail->count = 1;	
			} 
		
				removeFromList(_map[i].addrList);
		}
	}
	
	node *current = _map[0].addrList->head;
	while(current != NULL)
	{
		fprintf(ofp, "%s, %d\n", current->word, current->count);
		current = current->next;
	}

	fclose(ofp);
	
	return;
}

int open_file(char *filename, FILE *fd)
{
	char mode = 'r';
	fd = fopen(filename, &mode);
	if (fd == NULL)
	{
		printf("ERROR: Invalid input file specified: %s\n", filename);
		exit(-1);
	}
	else
	{
		return 0;
	}
}

int search_list(list *l, node *n)
{
	
	node *tempnode = malloc(sizeof(node));
	tempnode->word = malloc(sizeof(char[100]));
	strcpy(tempnode->word, n->word);
	tempnode->count = n->count;
	
	if (l->size == 0 || l->head == NULL || n == NULL)
	{
		return 1;
	}
	node *current = l->head;
	
	while(current != NULL) {
		if(strcmp(tempnode->word, current->word) == 0) 
		{				
			current->count = current->count + n->count;		
			return 0;
		}
		
		current = current->next;
	}
	
	return 1;
}

void addToList(list *l, node *n)
{	

	node *tempnode = malloc(sizeof(node));
	tempnode->word = malloc(sizeof(char[100]));
	strcpy(tempnode->word, n->word);
	tempnode->count = n->count;
	if(l->size == 0 && l->head == NULL)
	{
		//l->head = n;
		//l->tail = n;
		//n->next = NULL;
		
		l->head = malloc(sizeof(node));		
		l->head->word = malloc(sizeof(char[100]));
		strcpy(l->head->word, tempnode->word);
		l->head->count = tempnode->count;
		l->tail = l->head;
		
	} //else if (l->head == l->tail){
		//l->head->next = n;
		//n->next = NULL;
		//l->tail = n;
	//}
	
	else
	{
		//node *prev = l->tail;
		//prev->next = n;
		//n->next = NULL;
		//l->tail = n;
		
		l->tail->next = malloc(sizeof(node));
		l->tail->next->word = malloc(sizeof(char[100]));
		strcpy(l->tail->next->word, tempnode->word);
		l->tail->next->count = tempnode->count;
		l->tail = l->tail->next;
		
	}
	
	l->size++;
}

//FIFO implementation
void removeFromList (list *l)
{
	if(l->size == 0)
	{
		Error("Removing from already empty list");
	}
	if(l->size == 1)
	{
		l->size = 0;
		return;
	}

	node *tempHead = l->head;
	l->head = l->head->next;
	free(tempHead);
	//l->head = NULL;

	l->size--;
}
	
char* removeSymbols(char *word)
{
	char *token;
	char* sym = "|]}[{';:,.?()!-_";
	char *saveptr;
	char *result = "";

	for(token = strtok_r(word, sym, &saveptr); token; token = strtok_r(NULL, sym, &saveptr))
	{
		result = token;
	}
	
	return result;
}

void lowercase(char *input)
{
	while(*input != '\0')
	{
		*input = tolower(*input);
		input++;
	}
	
}