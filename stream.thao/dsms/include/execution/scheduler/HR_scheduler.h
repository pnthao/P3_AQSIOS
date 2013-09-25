#ifndef _HR_SCHEDULER_
#define _HR_SCHEDULER_

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

#include <vector>

namespace Execution {
	class HRScheduler : public Scheduler {
	private:
		static const unsigned int MAX_OPS = 1000;
		
		// Operators that we are scheduling
		std::vector <Operator *> ops;

		// Source ops that we are scheduling
		std::vector <Operator *> sr_ops;

		// Number of operators that we have to schedule
		unsigned int numOps;
		
		//Number of source operators we have to schedule
		unsigned int numSrOps;

		//the length of a cycle in tuples
		// i.e. the number of tuples that should be 
		//processed before the system expires the tickets
		int cycle_length;
		
		bool bStop;
		
	public:
		HRScheduler  ();		
		virtual ~HRScheduler ();
		
		// Inherited from Scheduler
		int addOperator (Operator *op);				
		int run (long long int numTimeUnits);
		int stop ();
		int resume ();

		// the number of operator refreshes that the system needs
		//to reach steady state --> the number of times the operators
		// need to refresh their priorities to reach steady state
		int STEADY_STATE;

		/**
		 * this method is used by the scheduler every 
		 * cycle to refresh the priorities
		 * @return int 0 success !0 failure
		 */
		int refreshPriorities();
		
		/** 
		 * this method is used to assign priorities to source
		 * and output operators.
		 * @return int 0 success !0 failure
		 */
		int assignPriToSrcOut();
		
		
		/**
		 * this method goes through the chain beginning at 
		 * currentOp and adjusts the priorities of all 
		 * operators according to multiplier.
		 */
		void adjustPrioritiesOfOutputs ( Operator * currentOp, double multiplier );
		/**
		 * this method goes over the operators
		 * and finds out if the system is in steady 
		 * state or not
		 */
		bool steadySystem ();		

	};
}

#endif
