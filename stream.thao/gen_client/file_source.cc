#ifndef _FILE_SOURCE_
#include "file_source.h"
#endif

#ifdef _DM_
#include <assert.h>
#define ASSERT(x) assert(x)
#else
#define ASSERT(x) {}
#endif

#include <string.h>
#include <stdlib.h>
#include <iostream>
using namespace std;

static const unsigned int MAX_LINE_SIZE =  1024;
static char lineBuffer [MAX_LINE_SIZE];

using Client::FileSource;

FileSource::FileSource (const char *fileName)
	: input (fileName, std::ios_base::in)
{
	return;
}

FileSource::~FileSource () {}

int FileSource::start ()
{
	int rc;
	
	// Input file not properly opened or something wrong
	if (!input.is_open () || input.bad ()) {
	  if ( !input.is_open() ) fprintf(stderr, "Not open ");
	  else fprintf(stderr, "bad");
	  fprintf(stderr, "cannot open file ");
	  return -1;
	}

	// Read the schema line & parse it
	input.getline (lineBuffer, MAX_LINE_SIZE);
	if ((rc = parseSchema (lineBuffer)) != 0) {
	  fprintf(stderr, "cannot parse schema");
	  return rc;
	}

	// compute offsets used in the tuple encodings
	if ((rc = computeOffsets ()) != 0) {
	  fprintf(stderr, "cannot compute offsets");
	  return rc;
	}

	fprintf(stdout, "DONE opening file");
	return 0;
}

static bool emptyLine (const char *line)
{
	for (; *line && isspace (*line); line++);
	
	return (*line == '\0');
}

int FileSource::getNext (char *&tuple, unsigned int &len, bool& isHeartbeat)
{
	int rc;
	
	isHeartbeat = false;
	
	len = tupleLen;
	
	// If there is a line parse it into a tuple & return
	if (input.good ()) {
		
		input.getline (lineBuffer, MAX_LINE_SIZE);
		
		if (emptyLine (lineBuffer)) {
			tuple = 0;
			len = 0;
		}
		
		else {
			if ((rc = parseTuple (lineBuffer)) != 0)
				return rc;
		
			tuple = tupleBuf;
		}
	}
	
	// EOF
	else if (input.eof ()) {		
		tuple = 0;
		len = 0;
	}
	
	// Some error
	else {
		ASSERT (input.bad());
		return -1;
	}
	
	return 0;
}

// Response Time Calculation By Lory Al Moakar
//overloaded the file source method 
int FileSource::getNext (char *&tuple, unsigned int &len, bool& isHeartbeat, unsigned long long int &cursysTime)
{
	int rc;
	
	isHeartbeat = false;
	
	len = tupleLen;
	
	// If there is a line parse it into a tuple & return
	if (input.good ()) 
	{

	    streampos pos=input.tellg();//By Shenoda Guirguis: to return back to it, if the time stamp is larger than current sysTime
		
		input.getline (lineBuffer, MAX_LINE_SIZE);
		
		if (emptyLine (lineBuffer)) {
			tuple = 0;
			len = 0;
		}
		else 
		{
			if ((rc = parseTuple (lineBuffer)) != 0)
			{
			  printf("Err: parsing a Tuple\n");
			  return rc;
		        }

			//By Shenoda Guirguis
			//printf("line: %s",lineBuffer);
			//printf("tuple parsed\n");
			tuple = tupleBuf;
			if(lineBuffer[0]!='i')    //if not first line
		        {
			    unsigned long long int Ts;
			    //memcpy(&inputTupleTs, tuple, TIMESTAMP_SIZE);
			    char tsstr[15];
			    int i =0;
			    while(i<len && lineBuffer[i]!=',' && lineBuffer[i]!='\0') 
			    {
		               tsstr [i] = lineBuffer[i];
			       i++;
			    }
			    tsstr[i]='\0';
			    //printf("got TS = %s\n", tsstr);
			    Ts = atoi(tsstr);
			    //printf("converted TS = %d\n", Ts);
			    if(Ts > cursysTime)
			    {		       
			        input.seekg(pos);
			        tuple = 0;
			        len = 0;
			    }
			 }
		}
	}
	
	// EOF
	else if (input.eof ()) {		
		tuple = 0;
		len = 0;
	}
	
	// Some error
	else {
		ASSERT (input.bad());
		return -1;
	}
	
	return 0;
}

//end of part 1 of response time calculation by LAM

//added by Thao Pham, to simulate the case when data is read from memory
int FileSource::getNext (char *&tuple, unsigned int &len, bool& isHeartbeat, unsigned long long int &cursysTime, bool dataInBuffer)
{
	int rc;

	isHeartbeat = false;

	len = tupleLen;

	char* lineBuf;
	
	//now attempt to read the tuple from the raw data buffer
	// If there is a line parse it into a tuple & return
	if (rawDataBuf.size()>0)
	{

		lineBuf = (char*)rawDataBuf.front();
		strcpy(lineBuffer,lineBuf);

		if (emptyLine (lineBuffer)) {
			tuple = 0;
			len = 0;
		}
		else
		{
			if ((rc = parseTuple (lineBuffer)) != 0)
			{
			  printf("Err: parsing a Tuple\n");
			  return rc;
		        }

			//By Shenoda Guirguis
			//printf("line: %s",lineBuffer);
			//printf("tuple parsed\n");
			tuple = tupleBuf;
			if(lineBuf[0]!='i')    //if not first line
		        {
			    unsigned long long int Ts;
			    //memcpy(&inputTupleTs, tuple, TIMESTAMP_SIZE);
			    char tsstr[15];
			    int i =0;
			    while(i<len && lineBuffer[i]!=',' && lineBuffer[i]!='\0')
			    {
		               tsstr [i] = lineBuffer[i];
			       i++;
			    }
			    tsstr[i]='\0';
			    //printf("got TS = %s\n", tsstr);
			    Ts = atoi(tsstr);
			    //printf("converted TS = %d\n", Ts);
			    if(Ts > cursysTime)
			    {
			        //the time hasn't come yet to read this tuple
			    	tuple = 0;
			        len = 0;
			    }
			    else
			    {
			    	//pop the line from the queue
			    	rawDataBuf.pop();
			    	//free the line buffer
			    	free(lineBuf);
			    }
			 }
		}

	}
	// EOF
	else {
		tuple = 0;
		len = 0;
	}

	return 0;
}


int FileSource::loadSourceData()
{
		//printf("load source data\n");		
		char * lineBuf = 0;
		while (input.good ())
		{

			lineBuf = (char*)malloc(MAX_LINE_SIZE);
			input.getline (lineBuf, MAX_LINE_SIZE);
			//printf("%s\n",lineBuf);			
			if(!emptyLine(lineBuf))
				rawDataBuf.push(lineBuf);
			else
			{
				free(lineBuf);
				lineBuf =0;
			}

		}
		if(!input.eof())
		{
			ASSERT (input.bad());
			return -1;
		}
	input.close();	
	return 0;
}
void FileSource::skip(int num_of_tuples)
{
	while (input.good() && num_of_tuples>0) 
	{   
		input.ignore(MAX_LINE_SIZE, '\n');
		//input.seekg(tupleLen , std::ios::cur);
		num_of_tuples --;
		
	}
		
}

int FileSource::end ()
{
  //HR scheduler by Lory Al Moakar
  //changing the semantics of this method
  //so that it returns if we reached the end of file or not
  if ( input.eof() ) 
    return 1;
  return 0;
  
  //end of HR scheduler by LAM
}

int FileSource::parseTuple (char *lineBuffer)
{
	char *begin, *end;
	int ival;
	float fval;	
	
	begin = end = lineBuffer;
	//printf("\n%s\n", lineBuffer);
	for (int a = 0 ; a < numAttrs ; a++) {

		// Seek a comma
		for (; *end && *end != ',' ; end++)
			;
		
		// Empty attribute
		if (begin == end)
			return -1;

		if (*end == '\0' && a != numAttrs - 1)
			return -1;
		
		*end = '\0';
		
		switch (attrTypes [a]) {
			
		case INT:
			
			ival = atoi (begin);
			memcpy (tupleBuf + offsets [a], &ival, sizeof (int));			
			break;
			
		case FLOAT:
			
			fval = atof (begin);
			memcpy (tupleBuf + offsets [a], &fval, sizeof (float));
			break;
			
		case CHAR:
			
			strncpy (tupleBuf + offsets [a], begin, attrLen [a]);
			tupleBuf [offsets [a] + attrLen [a] - 1] = '\0';
			break;
			
		case BYTE:
			
			tupleBuf [offsets [a]] = *begin;
			break;
			
		default:
			
			ASSERT (0);
			break;
		}
		
		begin = ++end;
	}
	
	return 0;
}
	
int FileSource::parseSchema (char *ptr)
{
	
	numAttrs = 0;	
	while (*ptr && numAttrs < MAX_ATTRS) {

		switch (*ptr++) {
		case 'i':
			
			attrTypes [numAttrs++] = INT;
			break;
			
		case 'f':
			
			attrTypes [numAttrs++] = FLOAT;
			break;
			
		case 'b':
			
			attrTypes [numAttrs++] = BYTE;
			break;
			
		case 'c':
			
			attrTypes [numAttrs] = CHAR;
			attrLen [numAttrs] = strtol (ptr, &ptr, 10);
			if (attrLen [numAttrs] <= 0)
				return -1;
			numAttrs ++;
			break;
			
		default:
			return -1;
		}
		
		if (*ptr && *ptr != ',')
			return -1;
		
		if (*ptr == ',')
			ptr ++;		
	}
	
	return 0;
}
	
int FileSource::computeOffsets ()
{
	int offset = 0;

	for (int a = 0 ; a < numAttrs ; a++) {
		offsets [a] = offset;
		
		switch (attrTypes [a]) {
		case INT:
			offset += sizeof(int);
			break;

		case FLOAT:
			offset += sizeof(float);
			break;

		case CHAR:
			offset += attrLen [a];
			break;

		case BYTE:
			offset ++;
			break;

		default:
			return -1;
		}
	}

	tupleLen = offset;
	
	return 0;
}
		
//armaDILos
streampos FileSource::getCurPos(){
	return input.tellg();
}
