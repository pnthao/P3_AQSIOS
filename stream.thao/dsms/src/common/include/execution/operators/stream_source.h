#ifndef _STREAM_SOURCE_
#define _STREAM_SOURCE_

/**
 * @file             stream_source.h
 * @date             June 4, 2004
 * @brief            Stream source operator
 */

#include <ostream>

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _TABLE_SOURCE_
#include "interface/table_source.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _PHY_OP_
#include "metadata/phy_op.h"
#endif

#ifndef _CPP_QUEUE_
#include <queue>
#endif

namespace Execution {

#ifdef _CTRL_LOAD_MANAGE_
	struct Drop_info {
		unsigned long long int effective_time;
		unsigned int drop;
	};
#endif	

	class StreamSource : public Operator {
	private:
		unsigned int   id;
		
		Queue         *outputQueue;
		StorageAlloc  *storeAlloc;
		
		// Attribute specification: these may not be in the input tuple
		// format, so we do not use the usual AEval for copying ... [[
		// explanation ]]
		struct Attr {
			Type type;
			unsigned int len;
		};
		
		// Specification of the attributes in the input stream
		Attr attrs [MAX_ATTRS];
		unsigned int numAttrs;

		static const unsigned int TIMESTAMP_OFFSET = 0;
		static const unsigned int DATA_OFFSET = TIMESTAMP_SIZE;
		unsigned int offsets [MAX_ATTRS];
		
		// Where do these go in the output ...
		Column outCols [MAX_ATTRS];
		
		// Source who is feeding us the tuples
		Interface::TableSource *source;
		
		Timestamp lastInputTs;	   
		Timestamp lastOutputTs;

		
		std::ostream& LOG;
		
				
		 
	public:
		
		StreamSource (unsigned int id, std::ostream& LOG);
		virtual ~StreamSource ();
		
		//load managing, by Thao Pham
		
		//for input rate computation
		unsigned long long int numOfIncomingTuples; 
		
		//timestamp of the tuple begining the rate computation cycles
		Timestamp startTs;
		
		//input rate - number of tuples/ time unit
		double inputRate;
		unsigned long int input_rate_time_unit;
		
		/*load coefficient, actual load caused by this input 
		 * is computed as <input rate>*<load coefficient>
		 * is calculated based on operators' "stable" cost per tuple 
		 */
		double loadCoefficient;
		
		/*snapshot load coefficient, which is coefficient calculated based on operators' 
		 * "snapshot, short-term" cost, refer to operator.h for more explanation
		 */
		double snapshot_loadCoefficient;
		//
		
		//effective coefficient: which is the coefficient calculated based on operators'
		//costs when the the system is under heavy load. This is used to calculate the amount of load to shed
		//since this is the expected load coefficient of the input after shedding is applied
		
		double effective_loadCoefficient;
		
		//for testing with purdue's load shedder only, making this drop-source
		unsigned int drop_percent;
		int dropCount;
		int tupleCount;
		//end of load managing, by Thao Pham
		
		int setOutputQueue (Queue *outputQueue);
		int setStoreAlloc (StorageAlloc *storeAlloc);
		int setTableSource (Interface::TableSource *tableSource);
		int addAttr (Type type, unsigned int len, Column outCol);
		
		int initialize ();
		
		int run (TimeSlice timeSlice);
		
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
		// end of part 1 of HR implementation by LAM
		//HR with ready by Lory Al Moakar
		int readyToExecute();
		//end of part 1 of HR with ready by LAM
		
		//load managing, by Thao Pham
		
		int setLoadCoefficient(double coefficient, double snapshot_coefficient, double effective_coefficient);
		
		//get the load of the input, i.e, <input rate>*<load coefficient>
		int getLoad(double& load, double& effective_load, double &source_load);
		
		/*the following 2 methods will be called by the load manager
		 * after each cycle
		 */
		//reset the inter-arrival time computation cycle 
		int resetInputRateComputationCycle();
		
		bool computeInputRate();
		
		//response time monitor
		
		long long int getLastInputTs()
		{
			return this->lastInputTs;
		}
		
		
		/*these two variables are used to keep track of number of tuples randomly dropped so far
		 *to make sure that exactly "dropPercent" percent of tuples are dropped.
		 *This might not be necessary though
		 *The variables is reset whenever tuplecount = 100  
		 */

#ifdef _CTRL_LOAD_MANAGE_		

		std::queue<unsigned long long int> drop_info_queue;
		void push_drop_info(unsigned long long int  drop, unsigned long long int time);
		FILE* ctrl_input_rates;
		double ctrl_current_rate;
		unsigned long long int ctrl_rate_effective_time;
		double get_incoming_tuples(unsigned long long int &cur_ts);
		double ctrl_sum ;
		double ctrl_num_cycle;
		unsigned long long int ctrl_last_ts;
		double ctrl_compensation;
		double ctrl_actual_total_incoming_tuples;
		double ctrl_estimated_total_incoming_tuples;
		bool ctrl_adjusted;
		double ctrl_checkpoint_estimated_total_incoming_tuples;
		unsigned long long int ctrl_checkpoint_ts;
		
#endif //_CTRL_LOAD_MANAGE_

		//end of load managing, by Thao Pham
	};
}

#endif
