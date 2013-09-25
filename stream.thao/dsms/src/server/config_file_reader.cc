#ifndef _CONFIG_FILE_READER_
#include "server/config_file_reader.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

using std::endl;

ConfigFileReader::ConfigFileReader (ostream &_LOG)
	: LOG (_LOG) {}

ConfigFileReader::~ConfigFileReader () {}

int ConfigFileReader::setConfigFile (const char *fileName)
{
	ASSERT (fileName);
	
	inputFile.open (fileName, std::ios_base::in);
	lineNo = 0;
	return 0;
}

static const int MAX_LINE = 1024;
static char lineBuf [MAX_LINE+1];

int ConfigFileReader::getNextParam (Param &param, ParamVal &val,
									bool &bFound)
{
	int rc;
	
	bFound = false;
	
	while (!bFound) {
		
		// Reached the end of file
		if (inputFile.eof ())
			return 0;
		
		// Some other error in the input stream
		if (!inputFile.good ()) {
			LOG << "ConfigFileReader: error reading config file" << endl;
			return -1;
		}
		
		// Get the next input line
		inputFile.getline (lineBuf, MAX_LINE);
		lineBuf [MAX_LINE] = '\0';
		
		// An empty line with just ws
		if (isEmpty (lineBuf))
			continue;
		
		// Comment line
		if (isCommentLine (lineBuf))
			continue;
		
		if ((rc = parseLine (lineBuf, param, val)) != 0) {
			LOG << "ConfigFileReader: error in config file at line = " 
				<< lineNo << endl;
			return rc;
		}
		
		bFound = true;
	}
	
	return 0;
}

bool ConfigFileReader::isEmpty (const char *line)
{
	for (; *line ; line++)
		if (!isspace(*line))
			return false;
	return true;
}

bool ConfigFileReader::isCommentLine (const char *line)
{
	return (*line == '#');
}

static const char *MEMORY_SIZE_P       = "MEMORY_SIZE";
static const char *QUEUE_SIZE_P        = "QUEUE_SIZE";
static const char *SHARED_QUEUE_SIZE_P = "SHARED_QUEUE_SIZE";
static const char *INDEX_THRESHOLD_P   = "INDEX_THRESHOLD";
static const char *RUN_TIME_P          = "RUN_TIME";
static const char *CPU_SPEED_P         = "CPU_SPEED";
// Response Time Calculation By Lory Al Moakar
//the text that the user uses to input the time_unit parameter
static const char *TIME_UNIT_P         = "TIME_UNIT";
//end of part 1 of response time calculation by LAM 

//load managing, by Thao Pham

static const char *INPUT_RATE_TIME_UNIT_P = "INPUT_RATE_TIME_UNIT";
static const char *HEADROOM_FACTOR_P = "HEADROOM_FACTOR";
static const char *DELAY_TOLERANCE_P = "DELAY_TOLERANCE";
static const char *INITIAL_GAP_P = "INITIAL_GAP";
static const char *NEXT_GAP_P = "NEXT_GAP";
static const char *LOAD_MANAGER_P = "LOAD_MANAGER";
//end of load managing, by Thao Pham

//Lottery scheduling by Lory Al Moakar
static const char *SCHEDULING_POLICY_P = "SCHEDULING_POLICY";
//end of part 1 of Lottery scheduling by LAM 

//Query Class Scheduling by Lory Al Moakar
// the number of query classes inputted by the user
static const char *QUERY_CLASSES_P="NUMBER_OF_QUERY_CLASSES";
// specifies the priority of a certain class
static const char *CLASS_PRIORITY_P="PRIORITY_OF_CLASS";
// are we honoring the user priorities as is ?
static const char * HONOR_PRIORITIES_P = "HONOR_PRIORITIES";

// the number of operator refreshes that the system needs
//to reach steady state --> the number of times the operators
// need to refresh their priorities to reach steady state
static const char * STEADY_STATE_P = "STEADY_STATE";

// the number of time units to run each QCHR scheduler for
//this is used by the QC_scheduler
static const char * QUOTA_P =  "SCHEDULER_QUOTA";

//added in order to signify if there is sharing of regular ops
// ( non- source ops ) among query classes or not
static const char * SHARING_AMONG_CLASSES_P = "SHARING_AMONG_CLASSES";

//end of part 1 of Query Class Scheduling by LAM
  

int ConfigFileReader::parseLine (const char *line,
								 Param      &param,
								 ParamVal   &val)
{
	const char *ptr, *begin;
	
	ptr = line;
	
	// eat white space
	for (; *ptr && isspace (*ptr) ; ptr++);
	
	// First word: parameter specifier
	begin = ptr;
	for (; *ptr && (isalpha (*ptr) || *ptr == '_') ; ptr++);
	
	if ((ptr - begin == 11) &&
		(strncmp(begin, MEMORY_SIZE_P, 11) == 0)) {
		param = MEMORY_SIZE;
	}
	
	else if ((ptr - begin == 10) &&
			 (strncmp(begin, QUEUE_SIZE_P, 10) == 0)) {
		param = QUEUE_SIZE;
	}
	
	else if ((ptr - begin == 17) &&
			 (strncmp(begin, SHARED_QUEUE_SIZE_P, 17) == 0)) {		
		param = SHARED_QUEUE_SIZE;
	}
	
	else if ((ptr - begin == 15) &&
			 (strncmp(begin, INDEX_THRESHOLD_P, 15) == 0)) {		
		param = INDEX_THRESHOLD;
	}
	
	else if ((ptr - begin == 8) &&
			 (strncmp (begin, RUN_TIME_P, 8) == 0)) {
		param = RUN_TIME;
	}

	else if ((ptr - begin == 9) &&
			 (strncmp(begin, CPU_SPEED_P, 9) == 0)) {
		param = CPU_SPEED;
	}
	
	// Response Time Calculation By Lory Al Moakar
	//detect time unit parameters
	else if ((ptr - begin == 9) &&
		 (strncmp(begin, TIME_UNIT_P, 9) == 0)) {
	  param = TIME_UNIT;
	}
	
	//end of part 2 of response time calculation by LAM

	//Lottery scheduling by Lory Al Moakar
	//detect the scheduling policy parameter
	else if ((ptr - begin == 17) &&
		 (strncmp(begin, SCHEDULING_POLICY_P, 17) == 0)) {
	  param = SCHEDULING_POLICY;
	}

	//end of part 2 of Lottery scheduling by LAM

	//Query Class Scheduling by Lory Al Moakar
	else if ( (ptr - begin == 23 )&& 
		  (strncmp(begin, QUERY_CLASSES_P, 23) == 0 )) {
	  param = QUERY_CLASSES;
	}
	else if ( (ptr - begin == 17 ) && 
		  (strncmp(begin,CLASS_PRIORITY_P, 17) == 0 )) {
	  param = CLASS_PRIORITY;
	}
	else if ( (ptr - begin == 16 )&& 
		  (strncmp(begin, HONOR_PRIORITIES_P, 16) == 0 )) {
	  param = HONOR_PRIORITIES;
	}
	else if ( (ptr - begin == 12 ) && 
		  (strncmp(begin,STEADY_STATE_P, 12) == 0 )) {
	  param = STEADY_STATE;
	}
	else if ( (ptr - begin == 15 ) && 
		  (strncmp(begin,QUOTA_P, 15) == 0 )) {
	  param = QUOTA;
	}
	
	else if( (ptr - begin == 21 ) && 
		  (strncmp(begin,SHARING_AMONG_CLASSES_P, 21) == 0 )) {
	  param = SHARING_AMONG_CLASSES;
	} 

	//end of part 2 of Query Class Scheduling by LAM
	
	//load managing parameters, by Thao Pham
	else if( (ptr - begin == 20 ) && 
		  (strncmp(begin,INPUT_RATE_TIME_UNIT_P, 20) == 0 )) {
	  param = INPUT_RATE_TIME_UNIT;
	}
	
	else if( (ptr - begin == 15 ) && 
		  (strncmp(begin,HEADROOM_FACTOR_P, 15) == 0 )) {
	  param = HEADROOM_FACTOR;
	 
	}
	
	else if( (ptr - begin == 15 ) && 
		  (strncmp(begin,DELAY_TOLERANCE_P, 15) == 0 )) {
	  param = DELAY_TOLERANCE;
	 
	}
	
	else if( (ptr - begin == 11 ) && 
		  (strncmp(begin,INITIAL_GAP_P, 11) == 0 )) {
	  param = INITIAL_GAP;
	 
	}
	
	else if( (ptr - begin == 8 ) && 
		  (strncmp(begin,NEXT_GAP_P, 8) == 0 )) {
	  param = NEXT_GAP;
	 
	}
	
	else if( (ptr - begin == 12 ) && 
		  (strncmp(begin,LOAD_MANAGER_P, 12) == 0 )) {
	  param = LOAD_MANAGER;
	 
	}
	//end of load managing parameters, by Thao Pham
	else {
		LOG << "ConfigFileReader: unknown parameter in line no "
			<< lineNo
			<< endl;
		return -1003;
	}
	
	// eat white space
	for (; *ptr && isspace (*ptr) ; ptr++);

	// Should read an '='
	if (*ptr != '=') {
		LOG << "ConfigFileReader: error in line no "
			<< lineNo
			<< endl;
		return -1;
	}

	// eat off '='
	ptr++;
	
	// eat white space
	for (; *ptr && isspace(*ptr) ; ptr++);

	if (*ptr == '\0') {
		LOG << "ConfigFileReader: error in line no "
			<< lineNo
			<< endl;		
		return -1;
	}
	
	if (param == MEMORY_SIZE        ||
		param == QUEUE_SIZE         ||
		param == SHARED_QUEUE_SIZE  ||
		param == CPU_SPEED
	    // Response Time Calculation By Lory Al Moakar
	    // if parameter type is time_unit then read an integer
	    || param == TIME_UNIT 
	    //end of part 3 of response time calculation by LAM
	    //Lottery scheduling by Lory Al Moakar
	    || param == SCHEDULING_POLICY
	    //end of part 3 of Lottery scheduling by LAM
	    //Query Class Scheduling by Lory Al Moakar
	    || param == QUERY_CLASSES
	    || param == HONOR_PRIORITIES
	    || param == STEADY_STATE
	    || param == QUOTA
	    || param == SHARING_AMONG_CLASSES
	    //end of part 3 of Query Class Scheduling by LAM
	    //load managing, by Thao Pham
	    ||param == INPUT_RATE_TIME_UNIT
	    ||param == INITIAL_GAP
	    ||param == NEXT_GAP
	    ||param == LOAD_MANAGER
	    //end of load managing, by Thao Pham
	    ) {
		
		val.ival = atoi (ptr);
	}
	
	else if (param == RUN_TIME) {
		val.lval = atoll(ptr);
		//	printf(" runtime is %lld\n", val.lval);
	}
	
	else {
		//including headroom factor and system capacity
		val.dval = atof (ptr);
	}
	
	return 0;
}
	
