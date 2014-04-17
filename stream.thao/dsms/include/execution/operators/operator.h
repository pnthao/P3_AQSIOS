#ifndef _OPERATOR_
#define _OPERATOR_

#ifndef _OP_MONITOR_
#include "execution/monitors/op_monitor.h"
#endif

//HR implementation by Lory Al Moakar
#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#include <stdio.h>

//end of part 1 of HR implementation by LAM

/**
 * @file          operator.h
 * @date          May 30, 2004
 * @brief         Operator interface, as seen by the Scheduler & Load Manager.
 */

typedef unsigned int TimeSlice;

namespace Execution {
//armaDiLos, by Thao Pham
enum OperatorStatus{
	ACTIVE,
	INACTIVE,
	START_PENDING, //for source ops: read and keep tuples in an input queue, wait for starting_TS from source node, for output ops: keep output tuples in the queue waiting for finish ack from source
	START_PREPARING,//for source ops: discarding tuples with ts<starting_TS and change itself to active status; for output ops: read and output queued output tuples to file
	STOP_PREPARING

};

//end of ArmaDILoS by Thao Pham

//HR implementation by Lory Al Moakar
  // the maximum number of inputs an operator can have
  static const unsigned int MAX_IN_BRANCHING = 2;
  
  // the aging constant called alpha that is used
  // in order to calculate the selectivity of an operator
  static const double ALPHA = 0.175;//0.175;
  
  //aging constant called BETA used to calculate the effective cost of an operator
 static const double BETA = 0.2;
  // the factor by which the cost of an operator would be considered an outlier
  // this factor is muliplied by the standard deviaton
  // if the new measured cost is off the mean by this much 
  //--> outlier is detected
  static const double OULIER_FACTOR = 1.5;

  //an enum that lists the types of execution operators
  // it helps the operator determine if its output
  // is a join operator or not. This is important to use
  // especially since the calculation of the mean_selectivity 
  // and cost differs if one of the operator's outputs is a 
  // join.
  enum OperatorKind {
    BIN_JOIN,
    BIN_STR_JOIN,
    DISTINCT,
    DSTREAM,
    EXCEPT,
    GROUP_AGGR,
    ISTREAM,
    OUTPUT,
    PARTN_WIN,
    PROJECT,
    RANGE_WIN,
    REL_SOURCE,
    ROW_WIN,
    RSTREAM,
    SELECT,
    SINK,
    STREAM_SOURCE,
    UNION,
    SYS_STREAM_GEN
  };


  //end of part 2 of HR implementation by LAM
#ifdef _MONITOR_
	
	/**
	 * An operator - the basic unit of execution & scheduling.
	 */
	
	class Operator : public Monitor::OperatorMonitor {
	public:
		virtual ~Operator() {}
		
		/**
		 * Run the operator for a specified amount of "time" indicated by
		 * timeSlice parameter.  Called by the scheduler
		 */ 
		virtual int run (TimeSlice timeSlice) = 0;
		
		// Response Time Calculation By Lory Al Moakar
		//used by the stream_source and the rel_source operators
		//to input tuples 
		//also used by the output operator to calculate response time 
		unsigned long long int system_start_time;

		//added by Thao Pham: keep track of the system start time in real time :
		unsigned long long int system_start_CPUtime;
		// a variable used to store the length of 

		// a time unit
		int time_unit;
		//time unit to interpret the input timestamp
		unsigned long int input_rate_time_unit;
		
		//end of part 1 of response time calculation by LAM
		
		//Lottery Scheduler By Lory Al Moakar
		//a variable used to store the priority of an operator
		//in case we are using a lottery scheduler
		double priority;

		// a variable used to store the number of tuples 
		// processed so far within this cycle
		long int num_tuples_processed;

		// this variable is only used by stream_source operators
		// and relsource operators.
		long int n_tuples_inputted;
		
		//a variable that holds the number of tickets that are 
		//currently assigned to this operator
		int old_n_tickets;
		
		//a variable that holds the number of tickets that should 
		//be assigned to this operator according to its
		//priority
		int new_n_tickets;
		
		//end of part 1 of lottery scheduler by LAM
		
		//HR implementation by Lory Al Moakar
		
		// the mean cost of this operator --> the cost of
		// this operator and mean cost of the operators 
		// downstream from it all the way till the output 
		//operator
		double  mean_cost;
		
		
		// the mean selectivity of this operator -->
		// the selectivity of this operator and the mean
		// selectivity of the all the operators downstream 
		// from it all the way till the output operator
		double mean_selectivity;

		// the local selectivity of the operator
		// --> the observed selectivity calculated based
		// on the input and output of this operator
		double local_selectivity;

		
		// the number of tuples outputted from this operator
		//during the current cycle
		// this is used in conjuction with the num_tuples_processed
		// in order to calculate the selectivity of this operator
		// at the end of this cycle
		int n_tuples_outputted;
		
   		
		// the number of tuples on average in a window if this
		//operator is a window operator
		double n_tuples_win;

		
		// the local cost of this operator --> the time
		// it took to process num_tuples_processed
		unsigned long long int local_cost;
		
		//the local cost per tuple --> the time it took
		// to process one tuple
		double local_cost_per_tuple;
		
		// the number of outputs this operator has -->
		// the number of operator paths it is shared in 
		unsigned int numOutputs;
		
		//an array of Operators that read off the output
		//queue of this operator
		Operator *outputs [MAX_OUT_BRANCHING];
		
		/// Operators which form the input to this operator
		Operator *inputs  [MAX_IN_BRANCHING];
		
		/// number of input operators
		unsigned int numInputs;
		
		// the ID of this operator --> it's position in the
		// original scheduler's array
		unsigned int operator_id;

		// the type of this operator
		//it has to be one of the types listed above in the 
		// OperatorKind enum
		unsigned int operator_type;

		// a boolean used in order to find out if this 
		// operator has been visited --> mean stats have 
		//been calculated or not.
		bool visited;


		// an integer that stores the number of times the stats 
		// have been refreshed during this run
		int n_refreshes;

		// a boolean used in order to determine if the 
		// stats have been refreshed before or not
		bool firstRefresh;
		

		/**
		 * This method is used in order to calculate the local 
		 * selectivity and the local cost per tuple of this operator
		 * @return bool true or false
		 * true when the statistics have changed and false otherwise
		 */ 
		virtual bool calculateLocalStats()=0;
		
		/** this method is used in order to calculate the priority
		 * of the operator. This is used in order to assign the priorities
		 * used by the scheduler to schedule operators.
		 */
		
		virtual void refreshPriority()=0;
		
		// FOR  OUTLIER DETECTION 
		// WE ARE USING THE B.P WELFORD METHOD TO CALCULATE 
		// MOVING STANDARD DEVIATIONS
		// SEE http://www.johndcook.com/standard_deviation.html
		// ANY COST THAT IS MORE THAN 2* STANDARD_DEVIATION AWAY FROM THE MEAN
		// IS CONSIDERED AN OUTLIER
		double M_k;
		double M_k_1;
		double k_measurements; // number of measurements
		double S_k;
		double avg_k;
		double stdev;
				

		//end of part 3 of HR implementation by LAM

		//Query Class Scheduling by Lory Al Moakar
		// the id of the query class that this operator belongs to
		int query_class_id;
		
		//end of part 1 of Query Class Scheduling by LAM
	
		//HR with ready by Lory Al Moakar
		virtual int readyToExecute()= 0;
		//end of part 1 of HR with ready by LAM
		
		//ArmaDILoS  by Thao Pham
		Timestamp startTupleTs; //used for dest node in query migration
		Timestamp stopTupleTs; //used for source node in query migration, this is the startTupleTs at the corresponding dest node

		OperatorStatus status;

		void setOperatorStatus(OperatorStatus s){
			//need mutex or not?
			status = s;
		}

		virtual void deactivate(){status = INACTIVE;};
		void deactivatePrecedingOps(){
			//TODO: all these implementation for ArmaDILoS now only works for plan without sharing
			deactivate();
			for(int i=0;i<numInputs;i++){
				inputs[i]->deactivatePrecedingOps();
			}
		}
		//end of ArmaDILoS by Thao Pham

		//load managing, by Thao Pham
		virtual int run_with_shedder (TimeSlice timeSlice)=0;
		bool isShedder;
		double input_load; //when isShedder is true, this operator is not a source load and therefore its input load is the input rate at the corresponding source * (product of all preceding ops)
//	    int run_with_shedder (TimeSlice timeSlice);
		int drop_percent;
		/*snapshot local cost per tuples. As opposed to the local_cost_per_tuple which applies
		 * some statistical methods to calculate a stable average cost and eliminate outlier
		 * this one captures the true average cost per tuple for the current short period
		 * and is to be used together with the "stable" one to manage system load
		 */
		 
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

		double snapshot_local_cost_per_tuple;
		
		double effective_cost;
		
		//load managing, by Thao Pham
		int setLoadCoefficient(double coefficient, double snapshot_coefficient, double effective_coefficient)
		{
			this->loadCoefficient = coefficient;
			this->snapshot_loadCoefficient = snapshot_coefficient;
			this->effective_loadCoefficient = effective_coefficient;
			return 0;
		}
		int getLoad(double& load, double& effective_load, double &source_load)
		{
			/*if(isShedder==false) {//real source op
				load = this->loadCoefficient * ((StreamSource*)this)->inputRate* (100.0-(double)this->drop_percent)/100.0;
				effective_load = effective_loadCoefficient*((StreamSource*)this)->inputRate*(100.0-(double)this->drop_percent)/100.0;
				source_load = this->effective_cost*((StreamSource*)this)->inputRate*(100.0-(double)this->drop_percent)/100.0;
			}
			else {*/
			load = this->loadCoefficient * this->input_load* (100.0-(double)this->drop_percent)/100.0;
			effective_load = effective_loadCoefficient*input_load*(100.0-(double)this->drop_percent)/100.0;
			source_load = this->effective_cost*input_load*(100.0-(double)this->drop_percent)/100.0;
			//printf ("\n shedder get load \n");
			return 0;
		}

		int setHeavilyLoaddedCost()
		{
			if(this->effective_cost > 0)
				this->effective_cost = (1-BETA)*(this->effective_cost) + BETA*this->snapshot_local_cost_per_tuple;
			else
				this->effective_cost = this->snapshot_local_cost_per_tuple;
			return 0;
		}
		
		//start a new local stats computation cycle
		int resetLocalStatisticsComputationCycle()
		{
			num_tuples_processed= 0;
 			n_tuples_outputted = 0;
  			local_cost = 0;
  			
#ifdef _CTRL_LOAD_MANAGE_
			ctrl_out_tuple_count = 0;
#endif
  			return 0;
		}
		
#ifdef _CTRL_LOAD_MANAGE_
//now I just implement the calculation of this variable in project, select, and output
//since I'm going to test the control based load shedder only with these operator
                                                          
unsigned long long int ctrl_out_tuple_count ;
double ctrl_num_of_queuing_tuples ;
double ctrl_load_coef;

#endif //_CTRL_LOAD_MANAGE_		 
		//end of load managing, by Thao Pham
	};
	
#else
	
	/**
	 * An operator - the basic unit of execution & scheduling.
	 */
	
	class Operator {
	public:
		virtual ~Operator() {}
		
		/**
		 * Run the operator for a specified amount of "time" indicated by
		 * timeSlice parameter.  Called by the scheduler
		 */ 
		virtual int run (TimeSlice timeSlice) = 0;
		
		// Response Time Calculation By Lory Al Moakar
		//used by the stream_source and the rel_source operators
		//to input tuples 
		//also used by the output operator to calculate response time 
		unsigned long long int system_start_time;
		
		//added by Thao Pham: keep track of the system start time in real time :
		unsigned long long int system_start_CPUtime;
		// a variable used to store the length of 

		// a time unit
		int time_unit;
		//end of part 2 of response time calculation by LAM 
		
		//Lottery Scheduler By Lory Al Moakar
		//a variable used to store the priority of an operator
		//in case we are using a lottery scheduler
		double priority;

		// a variable used to store the number of tuples 
		// processed so far within this cycle
		long int num_tuples_processed;
		
		// this variable is only used by stream_source operators
		// and relsource operators.
		long int n_tuples_inputted;
		
		//end of part 2 of lottery scheduler by LAM
		
		//HR implementation by Lory Al Moakar
		
		// the mean cost of this operator --> the cost of
		// this operator and mean cost of the operators 
		// downstream from it all the way till the output 
		//operator
		double  mean_cost;
		
		
		// the mean selectivity of this operator -->
		// the selectivity of this operator and the mean
		// selectivity of the all the operators downstream 
		// from it all the way till the output operator
		double mean_selectivity;

		// the local selectivity of the operator
		// --> the observed selectivity calculated based
		// on the input and output of this operator
		double local_selectivity;

		
		// the number of tuples outputted from this operator
		//during the current cycle
		// this is used in conjuction with the num_tuples_processed
		// in order to calculate the selectivity of this operator
		// at the end of this cycle
		int n_tuples_outputted;
		

		// the number of tuples on average in a window if this
		//operator is a window operator
		int n_tuples_win;

		// the local cost of this operator --> the time
		// it took to process num_tuples_processed
		unsigned long long int local_cost;
		
		//the local cost per tuple --> the time it took
		// to process one tuple
		double local_cost_per_tuple;
		
		// the number of outputs this operator has -->
		// the number of operator paths it is shared in 
		unsigned int numOutputs;
		
		//an array of Operators that read off the output
		//queue of this operator
		Operator *outputs [MAX_OUT_BRANCHING];
		
		/// Operators which form the input to this operator
		Operator *inputs  [MAX_IN_BRANCHING];
		
		/// number of input operators
		unsigned int numInputs;
		
		// the ID of this operator --> it's position in the
		// original scheduler's array
		unsigned int operator_id;

		// the type of this operator
		//it has to be one of the types listed above in the 
		// OperatorKind enum
		unsigned int operator_type;

		// a boolean used in order to find out if this 
		// operator has been visited --> mean stats have 
		//been calculated or not.
		bool visited;

		// an integer that stores the number of times the stats 
		// have been refreshed during this run
		int n_refreshes

		// a boolean used in order to determine if the 
		// stats have been refreshed before or not
		bool firstRefresh;
		
	  	/**
		 * This method is used in order to calculate the local 
		 * selectivity and the local cost per tuple of this operator
		 * @return  bool true or false
		 * true when the statistics have changed and false otherwise
		 */ 
		virtual bool calculateLocalStats()=0;
		
		/** this method is used in order to calculate the priority
		 * of the operator. This is used in order to assign the priorities
		 * used by the scheduler to schedule operators.
		 */
		
		virtual void refreshPriority()=0;
		
		// FOR  OUTLIER DETECTION 
		// WE ARE USING THE B.P WELFORD METHOD TO CALCULATE 
		// MOVING STANDARD DEVIATIONS
		// SEE http://www.johndcook.com/standard_deviation.html
		// ANY COST THAT IS MORE THAN 2* STANDARD_DEVIATION AWAY FROM THE MEAN
		// IS CONSIDERED AN OUTLIER
		double M_k;
		double M_k_1;
		double k_measurements; // number of measurements
		double S_k;
		double avg_k;
		double stdev;

		//end of part 4 of HR implementation by LAM


		//Query Class Scheduling by Lory Al Moakar
		// the id of the query class that this operator belongs to
		int query_class_id;
		
		//end of part 2 of Query Class Scheduling by LAM 
		//HR with ready by Lory Al Moakar
		virtual int readyToExecute()= 0;
		//end of part 1 of HR with ready by LAM
		
		//load managing, by Thao Pham

		bool isShedder;
		double input_load; //when isShedder is true, this operator is not a source load and therefore its input load is the input rate at the corresponding source * (product of all preceding ops)
		//virtual int run_with_shedder (TimeSlice timeSlice) = 0;
		int drop_percent;
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
		/*snapshot local cost per tuples. As opposed to the local_cost_per_tuple which applies
		 * some statistical methods to calculate a stable average cost and eliminate outlier
		 * this one captures the true average cost per tuple for the current short period
		 * and is to be used together with the "stable" one to manage system load
		 */
		 
		double snapshot_local_cost_per_tuple;
		
		double effective_cost;
		
		//load managing, by Thao Pham
		int setLoadCoefficient(double coefficient, double snapshot_coefficient, double effective_coefficient)
		{
			this->loadCoefficient = coefficient;
			this->snapshot_loadCoefficient = snapshot_coefficient;
			this->effective_loadCoefficient = effective_coefficient;
			return 0;
		}
		int getLoad(double& load, double& effective_load, double &source_load)
		{
			/*if(isShedder==false) {//real source op
				load = this->loadCoefficient * ((StreamSource*)this)->inputRate* (100.0-(double)this->drop_percent)/100.0;
				effective_load = effective_loadCoefficient*((StreamSource*)this)->inputRate*(100.0-(double)this->drop_percent)/100.0;
				source_load = this->effective_cost*((StreamSource*)this)->inputRate*(100.0-(double)this->drop_percent)/100.0;
			}
			else {*/
			load = this->loadCoefficient * this->input_load* (100.0-(double)this->drop_percent)/100.0;
			effective_load = effective_loadCoefficient*input_load*(100.0-(double)this->drop_percent)/100.0;
			source_load = this->effective_cost*input_load*(100.0-(double)this->drop_percent)/100.0;

			return 0;
		}

		int setHeavilyLoaddedCost()
		{
			if(this->effective_cost > 0)
				this->effective_cost = (1-BETA)*(this->effective_cost) + BETA*this->snapshot_local_cost_per_tuple;
			else
				this->effective_cost = this->snapshot_local_cost_per_tuple;
			return 0;
		}
		
		//start a new local stats computation cycle
		int resetLocalStatisticsComputationCycle()
		{
			num_tuples_processed= 0;
 			n_tuples_outputted = 0;
  			local_cost = 0;
  			
#ifdef _CTRL_LOAD_MANAGE_
			ctrl_out_tuple_count = 0;
#endif
		}
		/*
		 * control-based load manager
		 */
#ifdef _CTRL_LOAD_MANAGE_
//now I just implement the calculation of this variable in project, select, and output
//since I'm going to test the control based load shedder only with these operator
                                                          
long unsigned int ctrl_out_tuple_count;
double ctrl_num_of_queuing_tuples;
double ctrl_load_coef;

#endif //_CTRL_LOAD_MANAGE_		 
		 	
	};
	//end of load managing, by Thao Pham
	

#endif
}
#endif
