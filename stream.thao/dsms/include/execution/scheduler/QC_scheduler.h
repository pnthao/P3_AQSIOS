#ifndef _QC_SCHEDULER_
#define _QC_SCHEDULER_

#ifndef _SCHEDULER_
#include "execution/scheduler/scheduler.h"
#endif

#ifndef _QC_HR_SCHEDULER_
#include "execution/scheduler/QCHR_scheduler.h"
#endif

#ifndef COMMUNICATOR_H_
#include "execution/communicator/communicator.h"
#endif

#ifndef NODE_INFO_H_
#include "execution/communicator/node_info.h"
#endif

#ifndef _OPERATOR_
#include "execution/operator/operator.h"
#endif


#include <vector>
#include <set>
#include<map>

namespace Execution {
	class QCScheduler : public Scheduler {
	private:
				
		// Schedulers that we are scheduling
		std::vector <QCHRScheduler *> scheds;

		//quota vector 
		std::vector <int> quota;
		
		bool bStop;
		
	public:
		QCScheduler  ();
		QCScheduler ( int n_classes );
		virtual ~QCScheduler ();
		
		// Inherited from Scheduler
		int addOperator (Operator *op);				
		int run (long long int numTimeUnits);
		int stop ();
		int resume ();

		// the number of operator refreshes that the system needs
		//to reach steady state --> the number of times the operators
		// need to refresh their priorities to reach steady state
		int STEADY_STATE;
		
		// the default value of how much to run each scheduler for
		int quota_default;
		
		//a variable that indicates if we are reducing the granularity
		// i.e. honoring the user's priorities 
		bool honor_priorities;

		/**
		 * this method goes over the schedulers
		 * and finds out if the system is in steady 
		 * state or not
		 */
		bool steadySystem ();
		
		/** 
		 * this method is used in order to set the state of the system to steady
		 * It is used by the QC_scheduler to set the state 
		 * of the individual schedulers to steady.
		 */
		void setSteadyState();
		
		// amardilos, by Thao Pham
		void updateNodeInfo(Communicator* comm);
		void updateActiveQueriesList(Communicator *comm);
		virtual void getSourceFilePos(std::set<int> queryIDs,std::map<Operator*,streampos> &sourceFilePos);
		virtual Operator* getSourceFromID(int sourceID);

  };
}

#endif
