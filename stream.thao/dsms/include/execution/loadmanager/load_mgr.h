/*
 * @file       load_mgr.h
 * @date       Mar 24, 2009
 * @brief      declare the load manager and related structure
 * @author     Thao N Pham
 */

#ifndef _LOAD_MGR_
#define _LOAD_MGR_

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

#ifndef _DROP_
#include "execution/operators/drop.h"
#endif

#ifndef _STREAM_SOURCE_
#include "execution/operators/stream_source.h"
#endif

#ifndef _OUTPUT_
#include "execution/operators/output.h"
#endif


#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#ifndef _PHY_OP_
#include "metadata/phy_op.h"
#endif
#include<iostream>
#include<set>
#include <map>

using namespace std;
namespace Execution {
	
	class Drop;
	class LoadManager {
	public:
		LoadManager();
		virtual ~LoadManager();
		//Array of candidate drop operators to be considered for shedding task
		Physical::Operator* drops[MAX_OPS_PER_TYPE];
		// Number of drop operators in the candidate list
		unsigned int numDrops;
		
		//array of stream inputs (stream sources)
		Physical::Operator* inputs[MAX_OPS_PER_TYPE];
		//number of stream inputs
		unsigned int numInputs;
		
		//array of outputs
		Physical::Operator *outputs[MAX_OPS_PER_TYPE];
		//number of outputs
		unsigned int numOutputs;
		
		static const int  MAX_OPS	=  2000;
		Physical::Operator *ops[MAX_OPS];
		unsigned int numOps;
		
		//the query networks to monitor - the list of used physical:: operator
		//Physical::Operator *usedOps;
			
		/*attempt to find a related input of a drop operator
		 * A stream input is related to a drop operator if all or part of 
		 * its data tuples go through the drop on their way
		 * this method is to be called recursively
		 */
		 int findRelatedInput_Drop(Physical::Operator* op, Execution::Operator *drop);
		
		/* attempt to find the list of related output of a drop operator
		 * A query output is related to a drop operator if all or part of its coming data tuples have gone through
		 * the drop.
		 * This method is to be called recursively.
		 */
		int findRelatedOutput_Drop(Physical::Operator* op, Drop* drop);
		
		
		/*compute the load coefficient (processing cost per tuple) for a segment of query
		 * started from an operator
		 * preSel: the product of selectivities of all the operators on the path from 
		 * the starting operator to this operator (each path is calculated separately) 
		 * For the starting operator (e.g, a source op), it should be called as computeLoadCoefficient(aPhysicalSource, 1,...)
		 * This method is to be called recursively.
		 */
		 
		 int computeLoadCoefficient (Physical::Operator *op, double source_rate, double preSel, double preCoef,double &coef, double& cur_coef, double &effective_coef);
	
		 
		 /*record that some (2,3?) previous cycle has abnormal costs, which may result in an significant
		  * increasing in response time for this cycle
		  */
		  int abnormal_cost;
		  
		  /*record the number of past cycles that the system load is stable, i.e,
		   * there is no or small change in the total shedding percentage
		   */
		   int numOfStableSheddingCycles;
		   
		   //the system is overloaded or not (any shedding applied?)
		   bool overloadded; 
		   
		   //the stable heavily-loadded-cost is available?
		   bool effective_cost_available; 
		
		//for experiments only, this file store the drop percentage calculated in each cycle
		FILE * sheddingLogFile ;
		
		//for ctrl-based load shedding model verification only, this is the system estimation 
		//of the output delay.
		FILE * delayEstimationFile;
		
		/*the length in nanosec of time unit for input rate
		 * this is used to translate the meaning of the difference in timestamps of input tuples
		 * For example, if this unit is 1000, then a timestamp difference of 1 means 1*1000 ns
		 */ 
		unsigned long int input_rate_time_unit;
		
		/*headroom factor. This headroom factor can be set to some arbitrary value between 0-1
		 * at the begining, and will be adjusted automatically during execution of the load shedding module.
		 */
		  
		double headroom_factor;
		
		//sensitivity analysis
		
		int initial_gap , next_gap;
		
		int load_manager_type;
		
		//delay tolerance, to be pass to all output operators
		
		double delay_tolerance;
		
		double critical_above_factor;
		double critical_below_factor;
		
		bool is_first_increasing;
		bool is_first_decreasing;

		/*when total load cannot be adjusted based on the estimated capacity (because there is disagreement),
		 * these are the amounts that is used to add/remove shedding.
		 * This amount is increased every time the same decision is made in the next cycle.
		 */
		int add_amount;
		int remove_amount;
		
		int add_times;
		int remove_times;

		/**
		 * Add a (candidate) drop operator to the list of drops that the load manager is keeping
		 */
		int addDropOperator (Physical::Operator *&op);
		
		//find the list of related input of a drop operator
		int findRelatedInputs_Drop(Physical::Operator *drop);
		
		//find the list of related output of a drop operator
		int findRelatedOutputs_Drop(Physical::Operator *drop);
		
		// Add an input to the list of inputs that the load manager is keeping
		int addSourceOperator (Physical::Operator *&op);
		
		//add an output to the list of outputs that the load manager is keeping
		int addOutputOperator (Physical::Operator *&op);
		
		//add an operator to the list of operators belonging to the managing of this load manager
		int addOperator (Physical::Operator *&op);
		
		/*
		 * set query plan -- the list of used ops
		 */
		 //int setQueryPlan(Physical::Operator *&usedOps);
		
		  			
		/**
		 * the following "run" method is called by the scheduler after a multiple of scheduling period to 
		 * evaluate system load and make load shedding decision.
		 * recomputeLoadCoefficient = false: re-evaluate system load 
		 * 							considering changes in input rates only  
		 */		
		 int run(bool recomputeLoadCoefficient, int schedulingtype = 1);
		
		 /*called every load managing cycle to inspect current system load,
		  * actually this should be a private method to be used by the public run
		  */
		 /*apply an additional total amount of drop to the set of drop operators
		  * more specifically, the total load of the system need to be decreased 
		  * by decrease_percent of the current value
		  */ 
		 int addDrop(int decrease_percent);
		 
		 /*remove an amount of drop to the set of drop operators
		  * more specifically, the total load of the system can be increased 
		  * by increase_percent of the current value
		  */
		 
		 int removeDrop(int increases_percent);
		 
		 //set the stable effective cost for all operator
		 int setHeavilyLoaddedCosts();
		 
		  
		 //double DetectExcessLoad();
		
		// Response Time Calculation By Lory Al Moakar
		// A variable used to store the time the scheduler
		// started executing
		//unsigned long long int startingTime;

		// a variable used to store the length of 
		// a time unit
		//int time_unit;
		//end of part 2 of response time calculation by LAM
		
				   
		  /*--PURDUE'S CONTROL-BASED LOAD SHEDDER
		   * RE-IMPLEMENTATION FOLLOWING YICHENG TU'S CODE
		   */
#ifdef _CTRL_LOAD_MANAGE_

		double ctrl_avg_cost;
		double ctrl_in_tuple_count; //the number of tuples actually get into the system (after shedding)
		double ctrl_out_tuple_count;
		double ctrl_total_tuple_count; //the total number of input tuples (before shedding)
		double ctrl_sum_total_input_tuples; //the sum of input tuples so far
		//double ctrl_shed_tuple_count;
	
		
		long long unsigned int ctrl_period;
		
		double ctrl_queue_len;
		
		double ctrl_avg_delay;	
		double ctrl_pre_error; //error(k-1)
		double ctrl_pre_u;// u(k-1)
		double ctrl_current_shed_factor;
		//these two are to calculate the value that the real delay is converging to 
		double ctrl_sum_real_delay;
		double ctrl_count_real_delay;
		

		double ctrl_headroom;//0.54; //no shed: for fix cost: 0.97; for var cost: 0.99
		static const double ctrl_b_0 = 0.4;
		static const double ctrl_b_1 = -0.31;
		static const double ctrl_a = -0.8;
//		static const long long unsigned int CONTROL_DELAY_TARGET = 20000000;//0000; // 2 ms as it is now in the Purdue code?
		
		long long unsigned int ctrl_last_ts;//to calculate the actual control period length 
		//bool ctrl_first_run;
				
		int runCtrlLoadMgr(); //run the Purdue control-based version
		int runDynamicCtrlLoadMgr(); // HCTRL: the Purdue control-based load shedder with Thao's addition to auto adjust the headroom factor
		int runExtendedCtrlLoadMgr();//ExtCTRL Thao's extension of the Purdue's control based load shedder to handle complex query
		int runExtendedDynamicCtrlLoadMgr(); //ExtHCTRL
		int runExtendedBaselineCtrlLoadMgr(); //extenfed baseline version
		int runBaselineLoadMgr1(); //the original baseline with long term avg cost calculation
		int runBaselineLoadMgr2();//the baseline with short-term avg cost
		double computeQueuingLoad();
		int setDropPercent(int percent, unsigned long long int effectiveTime);
		void incoming_tuples_monitor();
		
		
		//load manager - scheduler synergy
		double avg_capacity_usage; //percentage of capacity usage
		int cycle_count; //count the number of load management cycles since the last capacity tracking reset
		//double headroom_temp;
		void resetCapacityUsageTracking(); 
		void updateAvgCapacityUsage(double totalLoad);
		//reset the value of query load stored in the  (physical) output operator
		void updateAvgQueryLoad();
		
		//ArmaDiLos
		void getSourceFilePos(std::set<int> queryIDs,std::map<Physical::Operator*,streampos> &sourceFilePos);
		void findSource(Physical::Operator* op, set<Physical::Operator*> &relatedSource);
		Physical::Operator* getSourceFromID(int sourceID);
		//Physical::Operator* findPhysicalOp(Operator* op); //find the physical op corresponding to the Execution Op
		void onStartTimestampSet(Physical::Operator*op, set<int> queryIDs);
		void onSourceCompleted(int queryID);

#endif //_CTRL_LOAD_MANAGE_	    

	
	};
}

#endif
