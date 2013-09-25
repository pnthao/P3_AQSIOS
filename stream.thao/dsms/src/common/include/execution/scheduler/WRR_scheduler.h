#ifndef _WEIGHTED_ROUND_ROBIN_
#define _WEIGHTED_ROUND_ROBIN_

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

namespace Execution {
	class WeightedRRScheduler : public Scheduler {
	private:
		static const unsigned int MAX_OPS = 5000;//100;
		
		// Operators that we are scheduling
		Operator *ops [MAX_OPS];
		
		//quota vector 
		std::vector <int> quota;
		
		// the default value of how much to run each scheduler for
		int quota_default;
		// Number of operators that we have to schedule
		
		unsigned int numOps;
		
		
		bool bStop;
		
	public:
		WeightedRRScheduler ();
		WeightedRRScheduler (int n_classes);			
		virtual ~WeightedRRScheduler();
		
		// Inherited from Scheduler
		int addOperator (Operator *op);				
		int run (long long int numTimeUnits);
		int stop ();
		int resume ();
	};
}

#endif
