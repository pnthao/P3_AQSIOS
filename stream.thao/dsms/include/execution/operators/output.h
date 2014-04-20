#ifndef _OUTPUT_
#define _OUTPUT_

#include <ostream>

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _QUERY_OUTPUT_
#include "interface/query_output.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _TYPES_
#include "common/types.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#include<queue>
#include<set>
#include<semaphore.h>

namespace Execution {
	
	class Output : public Operator {
	private:
		unsigned int   id;
		Queue         *inputQueue;
		
		StorageAlloc *inStore;
		
		// My internal representation of an attribute: obvious semantics:
		struct Attr {
			Type type;
			unsigned int len;
		};
		
		//----------------------------------------------------------------------
		// Schema specification of the stream/relation that I am
		// outputting.
		//----------------------------------------------------------------------
		
		// Number of attributes in the schema
		unsigned int numAttrs;
		
		// Details of attributes.
		Attr attrs [MAX_ATTRS];
		
		// Columns used to access the attributes in the input tuples 
		Column inCols [MAX_ATTRS];
		
		//----------------------------------------------------------------------
		// Encoding details
		//----------------------------------------------------------------------

		// Buffer that I use to encode output tuples
		static const unsigned MAX_BUFFER_SIZE = 512;
		char buffer [MAX_BUFFER_SIZE];		
		
		// Length of the tuples that I encode & send to the output.  It is
		// fixed since we do not have varying length attributes
		unsigned int tupleLen;
		
		// Offsets where the attributes are encoded
		unsigned int offsets [MAX_ATTRS];
		
		// timestamp appears first ...
		static const unsigned int TIMESTAMP_OFFSET = 0;
		
		// ... effect comes after timestamp ...
		static const unsigned int EFFECT_OFFSET = TIMESTAMP_SIZE;

		// Size of the effect
		static const unsigned int EFFECT_SIZE = sizeof(char);
		
		// ... then come the data attributes.
		static const unsigned int DATA_OFFSET = TIMESTAMP_SIZE + EFFECT_SIZE;
		
        // How do I encode PLUS / MINUS (specified in the QueryOutput interface)
		static const int PLUS_ENCODING  = 1;
		static const int MINUS_ENCODING = 2;
		
		//----------------------------------------------------------------------
		
		// Object that accepts my output.
		Interface::QueryOutput *output;
		
#ifdef _DM_
		Timestamp   lastInputTs;
#endif

		std::ostream& LOG;
		
	public:
		Output(unsigned int id, std::ostream& LOG);
		virtual ~Output();
		
		//----------------------------------------------------------------------
		// Initialization methods
		//----------------------------------------------------------------------		
		int setInputQueue (Queue *inputQueue);
		int setInStore (StorageAlloc *store);
		int setQueryOutput (Interface::QueryOutput *output);
		int addAttr(Type type, unsigned int len, Column inCol);		
		int initialize();
		
		int run(TimeSlice timeSlice);		
		
		//HR implementation by Lory Al Moakar
		/**
		 * This method is used in order to calculate the local 
		 * selectivity and the local cost per tuple of this operator
		 * @return  bool true or false
		 * true when the statistics have changed and false otherwise
		 */ 
		bool calculateLocalStats();
		//load manager, by Thao Pham
		int run_with_shedder (TimeSlice timeSlice);
		//end of load manager, by Thao Pham

		/** this method is used in order to calculate the priority
		 * of the operator. This is used in order to assign the priorities
		 * used by the scheduler to schedule operators.
		 */
		
		void refreshPriority();

		// this variable is used so that the output operator
		// can distinguish between steady state and non steady state.
		bool steady_state;

		//a variable that shows how many times the input queue of the 
		//output operator was full.
		int n_full_queue;
		// end of part 1 of HR implementation by LAM
		//HR with ready by Lory Al Moakar
		int readyToExecute();
		//end of part 1 of HR with ready by LAM
			
		//load manager, by Thao Pham

		//end of load manager, by Thao Pham
		
		//average response time calculation, by Thao Pham
		double delay_tolerance;
		
		double sum_response_time;
		double avg_response_time;
		
		//minimum responsetime observed, assumed to be equal to processing time
		double std_response_time; 
		
		/*number of tuples processed in this cycle of calculating average response time
		 * may be different from that of calculating cost and selectivity
		 */
		int num_tuples_rt;
		
		/*previous avg response time, used to compute the slope*/
		double pre_avg_responsetime;

		/*previous ts when computing the avg response time, used to compute the slope*/
		unsigned long long int pre_rt_timestamp;

		/*timestamp of the first and the last tuples in the current batch of tuples to calculate the average response time*/
		unsigned long long int rt_first_ts;
		unsigned long long int rt_last_ts;

		/*current slope of the response time*/
		double rt_slope;

		/*compute response time based on statistics collects during the previous period
		 * and reset the response time calculation cycle */
		bool computeAvgResponseTime();
		
		/*check if accummulated delay observed?*/
		bool isAbnormalResponseTimeObserved();
		bool isCriticallyAbnormalResponseTimeObserved(double gap);
		bool isDecreasing(double gap);
		bool isIncreasing(double gap);
		bool isAccummulatedDelayObserved(double systemCapacityExpandingFactor);
		bool isSignificantlyBelowTargetObserved(double gap);
		//end of avg response time calculation, by Thao Pham 
		
		//ArmaDILoS
		int run_in_start_pending(TimeSlice timeSlice);
		int run_in_start_preparing(TimeSlice timeSlice);
		std::queue<char*> pending_tuples;
		void deactivate();

		sem_t *sem_outputfinish;
		pthread_mutex_t* mutex_outputIDs;
		std::set<int>*outputIDs;
	};
}

#endif
