#ifndef _QC_HR_SCHEDULER_
#define _QC_HR_SCHEDULER_

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

#include <vector>

namespace Execution {
	class QCHRScheduler : public Scheduler {
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

		//declare a variable that counts the number of tuples processed 
		//in this cycle
		int tuples_in_cycle;
		//number of times the priorities of the operators have been refreshed
		int n_refreshes;
		//number of times the ops queue was ready to execute
		int n_ready;
		//number of times we had to check the next operator to execute
		int n_not_ready;
		// the number of operator refreshes that the system needs
		//to reach steady state --> the number of times the operators
		// need to refresh their priorities to reach steady state
		int STEADY_STATE;
		
	public:
		QCHRScheduler  ();		
		virtual ~QCHRScheduler ();
		
		// Inherited from Scheduler
		int addOperator (Operator *op);				
		int run (long long int numTimeUnits);
		int stop ();
		int resume ();

		
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
		 * this method is used by the QC_scheduler in order to initialize
		 * the state of this scheduler.
		 * @return int 0 success !0 failure
		 */
		
		int initialize(long long int starting_t, int time_u,int st_state);

		/** 
		 * this method is used in order to set the state of the system to steady
		 * It is used by the QC_scheduler to set the state 
		 * of the individual schedulers to steady.
		 */
		void setSteadyState();
		
		//end of Query Class Scheduling by LAM
		

		/**
		 * this method goes over the operators
		 * and finds out if the system is in steady 
		 * state or not
		 */
		bool steadySystem ();	

		

	};
}

#endif
