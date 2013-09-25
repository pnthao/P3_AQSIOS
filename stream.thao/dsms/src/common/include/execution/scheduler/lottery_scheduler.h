#ifndef _LOTTERY_SCHEDULER_
#define _LOTTERY_SCHEDULER_

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

namespace Execution {
	class LotteryScheduler : public Scheduler {
	private:
		static const unsigned int MAX_OPS = 100;
		
		// Operators that we are scheduling
		Operator *ops [MAX_OPS];

		//an array that holds the number of tickets held
		//by each operator
		int n_tickets [MAX_OPS];
		  
		// Number of operators that we have to schedule
		unsigned int numOps;

		//max of operator priorities
		double max_op_priorities;

		//the length of a cycle in tuples
		// i.e. the number of tuples that should be 
		//processed before the system expires the tickets
		int cycle_length;
		
		bool bStop;
		
	public:
		LotteryScheduler ();		
		virtual ~LotteryScheduler ();
		
		// Inherited from Scheduler
		int addOperator (Operator *op);				
		int run (long long int numTimeUnits);
		int stop ();
		int resume ();
	};
}

#endif
