#ifndef _LOTTERY_SCHEDULER_ID
#define _LOTTERY_SCHEDULER_ID

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

#include <vector>

namespace Execution {
	class LotterySchedulerID : public Scheduler {
	private:
		static const unsigned int MAX_OPS = 100;
		
		// Operators that we are scheduling
		Operator *ops [MAX_OPS];

		//a vector that holds the operator id that holds 
		//each ticket number ( the index of the vector)
		std::vector <unsigned int> tickets;
		  
		// Number of operators that we have to schedule
		unsigned int numOps;

		//max of operator priorities
		double max_op_priorities;

		//the length of a cycle in tuples
		// i.e. the number of tuples that should be 
		//processed before the system reassigns the tickets
		int cycle_length;
		
		bool bStop;
		
	public:
		LotterySchedulerID ();		
		virtual ~LotterySchedulerID ();
		
		// Inherited from Scheduler
		int addOperator (Operator *op);				
		int run (long long int numTimeUnits);
		int stop ();
		int resume ();
	};
}

#endif
