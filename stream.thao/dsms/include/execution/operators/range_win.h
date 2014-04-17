#ifndef _RANGE_WIN_
#define _RANGE_WIN_

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _WIN_SYN_
#include "execution/synopses/win_syn.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

namespace Execution {
	class RangeWindow : public Operator {
	private:
		/// system wide id
		unsigned int id;

		/// System log
		std::ostream &LOG;
		
		Queue          *inputQueue;
		Queue          *outputQueue;
		TimeDuration    windowSize;
		WindowSynopsis *winSynopsis;
		StorageAlloc   *inStore;
		
		// Used for heartbeats
		Timestamp       lastInputTs;
		Timestamp       lastOutputTs;
		
		// Used to handle stalls.  [[ Explanation ]]
		bool            bStalled;
		Element         stalledElement;
		
	public:		
		RangeWindow (unsigned int id, std::ostream &LOG);
		virtual ~RangeWindow ();
		
		int setInputQueue (Queue *inputQueue);
		int setOutputQueue (Queue *outputQueue);
		int setInStore (StorageAlloc *inStore);
		int setWindowSize (unsigned int windowSize);
		int setWindowSynopsis (WindowSynopsis *winSynopsis);
		
		int run(TimeSlice timeSlice);
		//load manager, by Thao Pham
		int run_with_shedder (TimeSlice timeSlice);
		//end of load manager, by Thao Pham
		//HR implementation by Lory Al Moakar
		/**
		 * This method is used in order to calculate the local 
		 * selectivity and the local cost per tuple of this operator
		 * @return bool true or false
		 * true when the statistics have changed and false otherwise
		 */ 
		bool calculateLocalStats();
		
		/** this method is used in order to calculate the priority
		 * of the operator. This is used in order to assign the priorities
		 * used by the scheduler to schedule operators.
		 */
		
		void refreshPriority();
		
		//a variable used to keep track of the interarrival time 
		// of the data stream entering this operator
		// This might not be accurate to estimate the 
		// interarrival time of the input stream in general
		//  since it depends
		// on the filters that process this tuple before 
		//it arrives at this operator
		double interarrivaltime;
		
		// end of part 1 of HR implementation by LAM
		//HR with ready by Lory Al Moakar
		int readyToExecute();
		//end of part 1 of HR with ready by LAM
		//ArmaDILoS, by Thao Pham
		int run_in_stop_preparing(TimeSlice timeSlice);
		void deactivate();

				//end of ArmaDILoS
	private:
		int clearStall();
		int expireTuples(Timestamp expTs);
		int expireTuplesInStopPreparing(Timestamp expTs);
		int clearStallInStopPreparing();
	};
}
#endif
