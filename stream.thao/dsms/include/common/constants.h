#ifndef _CONSTANTS_
#define _CONSTANTS_

/**
 * @file        constants.h
 * @date        May 20, 2004
 * @brief       Various constants assumed in the system
 */

// Maximum number of registered (named) tables (streams/relations) in the system.
#define MAX_TABLES          1000

// Maximum number of queries that can be registered
#define MAX_QUERIES         1000

// Maximum number of attributes per (registered) table.
#define MAX_ATTRS           10

// Maximum number of operators reading from an operator
#define MAX_OUT_BRANCHING   10

//load managing, by Thao Pham
// maximum number of operators of a type
#define MAX_OPS_PER_TYPE				1000

//control period, 0.5sec, in nanosec
#define CONTROL_PERIOD    			150000000	//150ms
#define CONTROL_DELAY_TARGET 			300000000      //300ms

//load manager and scheduler synergy
#define SCHED_ADJUSTMENT_CYCLE		10 //ten load management cycle
//end of by Thao Pham, maximum number of operators of a type


		

#endif
