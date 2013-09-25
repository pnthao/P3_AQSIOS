#ifndef _SCHEDULER_
#define _SCHEDULER_

 #include <iostream>
#include <stdlib.h> 
#include <math.h>

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

// Response Time Calculation By Lory Al Moakar
#ifndef _TIMER_
#include "execution/monitors/timer.h"
#endif
//end of part 1 of response time calculation by LAM

//load manager, by Thao Pham
#ifndef _LOAD_MGR_
#include "execution/loadmanager/load_mgr.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif


//end of part 1 of load mamaging, by Thao Pham

namespace Execution {
	class Scheduler {
	public:
		/**
		 * Add a new operator to schedule
		 */
		virtual int addOperator (Operator *op) = 0;
		
		/**
		 * Schedule the operators for a prescribed set of time units.
		 */		
		virtual int run(long long int numTimeUnits) = 0;
		
		virtual int stop () = 0;
		virtual int resume () = 0;
		
		// Response Time Calculation By Lory Al Moakar
		// A variable used to store the time the scheduler
		// started executing
		unsigned long long int startingTime;

		// a variable used to store the length of 
		// a time unit
		int time_unit;
		//end of part 2 of response time calculation by LAM
					
		// an array that holds the priorities of the query classes
		// as inputted by the user
		
		//Query Class Scheduling by Lory Al Moakar
	// the maximum number of query classes in the system
		static const int MAX_QUERY_CLASSES =  10 ;
		double query_class_priorities [MAX_QUERY_CLASSES];
		double original_query_class_priorities[MAX_QUERY_CLASSES];
		// the number of query classes inputted by the user
		int n_query_classes;
		//end of Query Class Scheduling by LAM
		
		//Load manager, by Thao Pham
		LoadManager* loadMgrs [MAX_QUERY_CLASSES]; //we have separate load managers for each of the class
		unsigned int num_loadMgrs;
		
		int addLoadManager(LoadManager* mgr)
		{
			loadMgrs[num_loadMgrs++] = mgr;
			return 0;
		}
		//for the control-based load shedder
		long long unsigned int ctrl_last_ts;
		
		//end of part 2 of load manager, by Thao Pham
		
		//load manager synergy, by Thao Pham
		int count_loadManagementCycle;
		
		void adjustPriority(int k, double amount)
		{
			//amount: percentage of system capacity that need to add/remove, in %
			
			double total_priorities = 0;
		  	for ( int i = 0; i < n_query_classes; i++ ) 
		    		total_priorities +=  original_query_class_priorities[i];
			
			double new_pri = query_class_priorities[k] + (total_priorities*amount)/100;
			//adjust the headroom factor
			loadMgrs[k]->headroom_factor = (new_pri/query_class_priorities[k])*loadMgrs[k]->headroom_factor;
			
			query_class_priorities[k] = new_pri;
			
			
		}
		
		double getCurrentPriority(int k)
		{
			double total_priorities = 0;
		  	for ( int i = 0; i < n_query_classes; i++ ) 
		    		total_priorities +=  original_query_class_priorities[i];
			//printf("total: %f\n", total_priorities);
			return (query_class_priorities[k]/total_priorities)*100; 
		}
		
		void ResourceBalancing()
		{
			
			double budget = 0;
			double demands[MAX_QUERY_CLASSES];
			
			//FISRT PASS: calculating avalaible budget
			for(int i=0;i<num_loadMgrs; i++)
			{
				if(loadMgrs[i]->avg_capacity_usage<=100)
				{	
					
					double supply = ((100-loadMgrs[i]->avg_capacity_usage)*getCurrentPriority(i))/100; 
					budget += supply;
					//printf("capacity usage: %f , current priority: %f , demand: %f \n", loadMgrs[i]->avg_capacity_usage, getCurrentPriority(i), demands[i]);		
					//take the redundant capacity from the class
					adjustPriority(i,-supply);
					demands[i] = 0;
				}
				else{
					demands[i] = ((loadMgrs[i]->avg_capacity_usage-100)*getCurrentPriority(i))/100;	
					//printf("capacity usage: %f , current priority: %f , demand: %f \n", loadMgrs[i]->avg_capacity_usage, getCurrentPriority(i), demands[i]);		
				}
							
				loadMgrs[i]->resetCapacityUsageTracking();
			}
			//printf("%0.1f \n", budget);
			//SECOND PASS: consider those that are running under its original priority and demanding more
			for(int i=0;i<num_loadMgrs;i++)
				if(demands[i]>0 && query_class_priorities[i]<original_query_class_priorities[i])
				{
					double oriGap = getCurrentPriority(i)*(original_query_class_priorities[i]
														/query_class_priorities[i] -1);
					if(demands[i] < oriGap)
					{
						adjustPriority(i,demands[i]);
						budget -=demands[i];
						demands[i] = 0;
					}
					else
					{
						query_class_priorities[i] = original_query_class_priorities[i];
						budget -= oriGap;
						demands[i] -=oriGap;
					}
				}
			
			//THIRD PASS: if budget > 0, consider classes that are still demand more capacity, from high to low priority
			if(budget>0){
				for (int i=0;i<num_loadMgrs;i++)
				{
					if(demands[i]>0){
						if(demands[i]> budget){
							adjustPriority(i,budget);
							demands[i] -= budget;
							budget = 0;
							break;
						}
						else{
							adjustPriority(i,demands[i]);
							budget -=demands[i];
							demands[i]=0;
						}	
					}
					if(budget <=0) break;
				}
				//if the budget is still >0, consider giving the budget to those class still running under priority
				if(budget>0)
				{
					for(int i=0;i<num_loadMgrs;i++)
					{
						if(query_class_priorities[i]<original_query_class_priorities[i])
						{
							double oriGap = getCurrentPriority(i)*(original_query_class_priorities[i]
															/query_class_priorities[i] -1);
							if(oriGap < budget)
							{
								query_class_priorities[i] = original_query_class_priorities[i];
								budget -= oriGap;
							}
							else
							{
								adjustPriority(i,budget);
								budget =0;
							}
						
						}
						if(budget<=0) break;	
					}	
				}		
			}
			//FORTH PASS: if budget is less than 0
			else //budget < 0
			{
				for (int i=num_loadMgrs-1;i>=0; i--) //low to high priority
				{
					if(query_class_priorities[i]>original_query_class_priorities[i])
					{
						double oriGap = getCurrentPriority(i)*(1- original_query_class_priorities[i]
														/query_class_priorities[i]);
						if(oriGap > -(budget))
						{
							adjustPriority(i,budget);
							budget = 0;
							break;
						}
						else
						{
							query_class_priorities[i] = original_query_class_priorities[i];
							budget += oriGap;
						}
					}
					if (budget>=0) break;
				}
					
			}
				
			
		}
		
	};
}

#endif
