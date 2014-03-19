#ifndef _WRR_SCHEDULER_
#define _WRR_SCHEDULER_

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

namespace Execution {
	class WRR_Scheduler : public Scheduler {
	private:
		static const unsigned int MAX_OPS = 5000;//100;
		
		// Operators that we are scheduling
		Operator *ops [MAX_OPS];
		
		// Number of operators that we have to schedule
		
		unsigned int numOps;
		
		
		bool bStop;
		
	public:
		WRR_Scheduler ();
		WRR_Scheduler (int n_classes);			
		virtual ~WRR_Scheduler();
		
		//quota vector 
		std::vector <int> quota;
		
		// the default value of how much to run each scheduler for
		int quota_default;
		// Inherited from Scheduler
		int addOperator (Operator *op);				
		int run (long long int numTimeUnits);
		int stop ();
		int resume ();
	};
}

#endif
