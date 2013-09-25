#ifndef _CONFIG_FILE_READER_
#define _CONFIG_FILE_READER_

#include <ostream>
#include <fstream>

using std::ostream;
using std::ifstream;

/**
 * Configuration file reader.  
 */

class ConfigFileReader {
 private:
	/// System log
	ostream &LOG;
	
	/// Input file stream
	ifstream inputFile;
	
	/// Line no which we are currently processing
	unsigned int lineNo;
	
 public:

	/**
	 * Parameters used to configure the server
	 */
	enum Param {
		MEMORY_SIZE,
		QUEUE_SIZE,
		SHARED_QUEUE_SIZE,
		INDEX_THRESHOLD,
		RUN_TIME,
		CPU_SPEED,
		// Response Time Calculation By Lory Al Moakar
		//a parameter used to specify the length of the time unit
		//in nanoseconds
		TIME_UNIT, 
		//end of part 1 of response time calculation by LAM 
		
		//load managing params, by Thao Pham
		
		//length in nanosec of time unit for input rate (input rate is in terms of num. of tuples/timeunit
		INPUT_RATE_TIME_UNIT,
		
		//headroom factor
		HEADROOM_FACTOR,
		
		//initial gap
		INITIAL_GAP,
		
		//next_gap
		NEXT_GAP,
		
		//delay tolerance
		DELAY_TOLERANCE,
		
		//type of load manager
		LOAD_MANAGER,
		
		//end of load managing params, by Thao Pham
		
		//Lottery scheduling by Lory Al Moakar
		// a parameter used to specify the scheduling policy
		// used to schedule the operators
		SCHEDULING_POLICY,
		//end of part 1 of Lottery scheduling by LAM
		
		//Query Class Scheduling by Lory Al Moakar
		// the number of query classes inputted by the user
		QUERY_CLASSES,
		// specifies the priority of a certain class
		CLASS_PRIORITY,
		// are we honoring the user priorities as is ?
		HONOR_PRIORITIES,

		// the number of operator refreshes that the system needs
		//to reach steady state --> the number of times the operators
		// need to refresh their priorities to reach steady state
		STEADY_STATE,
		
		// the number of time units to run each QCHR scheduler for
		//this is used by the QC_scheduler
		QUOTA,
		//added in order to signify if there is sharing of regular ops
		// ( non- source ops ) among query classes or not
		SHARING_AMONG_CLASSES

		//end of Query Class Scheduling by LAM
		
	};
	
	/**
	 * The value of a parameter: could be an integer or a double
	 */ 
	union ParamVal {
		int ival;
		double dval;
		long long int lval;
	};

	ConfigFileReader (ostream &LOG);
	~ConfigFileReader ();
	
	/**
	 * Set the configuration file
	 */
	int setConfigFile (const char *fileName);
	
	/**
	 * Get the next parameter and its value from the config file
	 */ 	
	int getNextParam (Param &param, ParamVal &val, bool &bFound);
	
 private:
	
	bool isEmpty (const char *line);
	bool isCommentLine (const char *line);
	
	int parseLine (const char  *lineBuf,
				   Param       &param,
				   ParamVal    &val);
};

#endif
