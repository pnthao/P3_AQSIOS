/*
 * @file       load_mgr.cc
 * @date       Mar 24, 2009
 * @brief      Implementation of the load manager
 * @author     Thao N Pham
 */
 #include <iostream>
 #include <math.h>
 #include <stdlib.h>
 #include <stdio.h>
#include <set>
using namespace std;

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _LOAD_MGR_
#include "execution/loadmanager/load_mgr.h"
#endif

using namespace Physical;
using namespace Execution;
LoadManager::LoadManager()
{
	this -> numDrops = 0;	
	//this -> usedOps = 0;
	this->numInputs = 0;
	this->numOutputs = 0;
	this->numOps = 0;
	this->abnormal_cost = 0;
	this->numOfStableSheddingCycles = 0;
	this->overloadded = false;
	this->effective_cost_available = false;
	this->critical_above_factor = 0.1;
	this->critical_below_factor = 0.1;
	this->is_first_decreasing = true;
	this->is_first_increasing = true;
	this->add_amount = 0; //%
	this->remove_amount =0; //%
	this->add_times = 0;
	this->remove_times = 0;
	sheddingLogFile = 0;
	delayEstimationFile = fopen("/home/thao/workspace/stream.test.thao/load_managing/Feb8/delay_estimation","wt");
	
	//headroom_temp = 0;
	
#ifdef _CTRL_LOAD_MANAGE_

	ctrl_avg_cost = 0.0;
	ctrl_in_tuple_count = 0; //the number of tuples actually get into the system (after shedding)
	ctrl_out_tuple_count = 0;
	ctrl_total_tuple_count = 0; //the total number of input tuples (before shedding)

	
	ctrl_queue_len = 0;
	
	ctrl_avg_delay = 0.0;	
	ctrl_pre_error = 0.0; //error(k-1)
	ctrl_pre_u = 0.0;
	
	ctrl_last_ts = 0;
	ctrl_current_shed_factor = 0;
	ctrl_sum_real_delay = 0;
	ctrl_count_real_delay = 0;
	ctrl_headroom = headroom_factor;
	ctrl_sum_total_input_tuples = 0;
	avg_capacity_usage = 0;
	cycle_count = 0;

#endif //_CTRL_LOAD_MANAGE_
}

LoadManager::~LoadManager() {
	if(sheddingLogFile)
		fclose(sheddingLogFile);
#ifdef _CTRL_LOAD_MANAGE_	
	if(delayEstimationFile)
		fclose(delayEstimationFile);
#endif
}

int LoadManager::addDropOperator(Physical::Operator *&op)
{
	ASSERT(op);
	if (numDrops == MAX_OPS_PER_TYPE)
		return -1;	
	drops[numDrops ++] = op;

	return 0;
} 
int LoadManager::addOutputOperator(Physical::Operator *&op)
{
	ASSERT(op);
	if (numOutputs == MAX_OPS_PER_TYPE){
		return -1;
	}
	outputs[numOutputs ++] = op;
	
	//pass the delay_tolerance constant to the output
	((Output*)op->instOp)->delay_tolerance = this->delay_tolerance;
	
	return 0;
	
} 
int LoadManager::addSourceOperator(Physical::Operator *&op)
{
	ASSERT(op);

	if (numInputs == MAX_OPS_PER_TYPE)
		return -1;	
	inputs[numInputs ++] = op;
	
	op->instOp->input_rate_time_unit = this->input_rate_time_unit;
	return 0;
} 

int LoadManager::addOperator(Physical::Operator *&op)
{	
	ASSERT(op);

	if (numInputs == MAX_OPS_PER_TYPE)
		return -1;	
	ops[numOps ++] = op;
	
	return 0;
}
/*int LoadManager::setQueryPlan(Physical::Operator *&usedOps)
{
	ASSERT(usedOps);
	this->usedOps = usedOps;
	return 0;
}*/

int LoadManager::run(bool recomputeLoadCoefficient, int schedulingtype)
{
	double totalLoad = 0;
	//double cur_totalLoad = 0;
	double totalSourceLoad = 0;
	double totalEffectiveLoad = 0;
	bool still_abnormal = false;
	double load = 0, source_load =0, effective_load =0;
	double systemCapacityExpandingFactor = 2;
	
	
/*******************
 * COMPUTE CURRENT LOAD
 ********************/	
	if(recomputeLoadCoefficient) 
	{
		//recompute load coefficient based on the newly collected costs and selectivities
		/*Physical::Operator* op;
		op = usedOps;*/
		double coef = 0, snapshot_coef = 0, effective_coef =0;
		
		if(schedulingtype ==1 || schedulingtype ==10) //  RR scheduler or WRR scheduler
		{	
			/*HR and QC schedulers compute the statistics already, so 
			 * the load manager shouldn't recompute and mistakenly reset the cycle,
			 * it just uses the statistic calculated by the scheduler
			 * For RR, which does not touch the statistic collector, the load manager
			 * has to manage the statistic calculation.
			 */
			  
			/*while (op)
			{
				op->instOp->calculateLocalStats();
				op->instOp->resetLocalStatisticsComputationCycle();
				op = op->next;
			}*/
			for(int i=0;i<this->numOps; i++)
			{
				ops[i]->instOp->calculateLocalStats();
				ops[i]->instOp->resetLocalStatisticsComputationCycle();
			}
		}
		else
		{
			for(int i=0;i<this->numOps; i++)
			{
				if(ops[i]->kind == PO_STREAM_SOURCE||ops[i]->kind==PO_OUTPUT){
					ops[i]->instOp->calculateLocalStats();
					ops[i]->instOp->resetLocalStatisticsComputationCycle();
				}
			}
		}
				
		for (unsigned int i = 0; i<this->numInputs; i++)
		{
			if(inputs[i]->kind==PO_STREAM_SOURCE){
				/*compute the new input rate during the last collecting cycle,
				* and reset for a new cycle
				*/
				((StreamSource*)this->inputs[i]->instOp)->computeInputRate();
				((StreamSource*)this->inputs[i]->instOp)->resetInputRateComputationCycle();

				computeLoadCoefficient(inputs[i],1,0,((((StreamSource*)inputs[i]->instOp)->inputRate)*(100- inputs[i]->instOp->drop_percent))/100.0, coef, snapshot_coef, effective_coef);
				this->inputs[i]->instOp->setLoadCoefficient(coef, snapshot_coef, effective_coef);

				if(snapshot_coef> 1.5*coef){
					abnormal_cost  = 2; //this information will be kept up to 2(?) cycles
					still_abnormal = true;
				}


				((StreamSource*)this->inputs[i]->instOp)->getLoad(load, effective_load, source_load);
				totalLoad += load;
				//printf("detailed load: %f\n", load);
				//cur_totalLoad += ((StreamSource*)this->inputs[i]->instOp)->getCurLoad();
				totalSourceLoad += source_load;
				if(effective_cost_available)
					totalEffectiveLoad += effective_load;
			}

		}

		for (unsigned int i = 0; i<this->numInputs; i++)
		{
			if(inputs[i]->instOp->isShedder ==true){
				computeLoadCoefficient(inputs[i],1,0,(inputs[i]->instOp->input_load*(100-inputs[i]->instOp->drop_percent))/100.0,coef, snapshot_coef, effective_coef);

				this->inputs[i]->instOp->setLoadCoefficient(coef, snapshot_coef, effective_coef);

				if(snapshot_coef> 1.5*coef){
					abnormal_cost  = 2; //this information will be kept up to 2(?) cycles
					still_abnormal = true;
				}


				this->inputs[i]->instOp->getLoad(load, effective_load, source_load);
				totalLoad += load;
				//printf("detailed load: %f\n", load);
				//cur_totalLoad += ((StreamSource*)this->inputs[i]->instOp)->getCurLoad();
				totalSourceLoad += source_load;
				if(effective_cost_available)
					totalEffectiveLoad += effective_load;
				
				 //reset the input_load
				inputs[i]->instOp->input_load =0;
			}
		}

	}
	else{ //DISREGARED: now it works correctly with the other option only
		//recompute the input rate and corresponding load only
		for (unsigned int i =0; i<this->numInputs; i++)
		{
			/*compute the new input rate during the last collecting cycle, 
			 * and reset for a new cycle
			 */
			 
			((StreamSource*)this->inputs[i]->instOp)->computeInputRate();
			((StreamSource*)this->inputs[i]->instOp)->resetInputRateComputationCycle();
		
			((StreamSource*)this->inputs[i]->instOp)->getLoad(load, effective_load, source_load);
			totalLoad += load;
			
			//cur_totalLoad += ((StreamSource*)this->inputs[i]->instOp)->getCurLoad();
			totalSourceLoad += source_load;
			if(effective_cost_available)
				totalEffectiveLoad += effective_load; 
		}
	} 
	
	
	/*if(effective_cost_available)
	{	
		totalLoad = totalEffectiveLoad;
	}*/
	printf("load: %f\n", totalLoad);
	updateAvgCapacityUsage(totalLoad);
	updateAvgQueryLoad();
/***********************
 * RESPONSE TIME MONITOR
***********************/	
	
	 //overload in the eye of response time monitor - abnormal response time
	bool abnormal_response_time = false; //delay target violated - OT
	bool critically_abnormal_response_time = false;
	bool accumulated_delay = false; // UT
	bool significantly_below_target = false;
	bool isIncreasing = false;
	bool isDecreasing = false;
	int numIncreasing = 0;
	int numDecreasing = 0;
	int numViolatingTarget =0;
	int numBelowTarget=0;

	//bool critically_abnormal_response_time = false; //really over
	for(unsigned int o = 0; o< numOutputs ;o++)
	{
		((Output*)outputs[o]->instOp)->computeAvgResponseTime();
		
		if(((Output*)outputs[o]->instOp)->isAbnormalResponseTimeObserved())
		{
			abnormal_response_time= true;
			numViolatingTarget++;

			if(((Output*)outputs[o]->instOp)->isCriticallyAbnormalResponseTimeObserved(critical_above_factor))
			{
				critically_abnormal_response_time = true;
				if((((Output*)outputs[o]->instOp)->isIncreasing(0)))
					//isIncreasing = true;
					numIncreasing++;
			}
			//break;
		}
		else if(((Output*)outputs[o]->instOp)->isSignificantlyBelowTargetObserved(critical_below_factor))
		{
			significantly_below_target = true;
			numBelowTarget++;
			if((((Output*)outputs[o]->instOp)->isDecreasing(0)))
				//isDecreasing = true;
				numDecreasing++;
		}
		if(((Output*)outputs[o]->instOp)->isAccummulatedDelayObserved(systemCapacityExpandingFactor))
		{
			accumulated_delay = true;  
		}
	}
	if(numIncreasing > numViolatingTarget/2)  isIncreasing = true;
	if(numDecreasing > numBelowTarget/2)  isDecreasing = true;

	if(abnormal_response_time) { significantly_below_target = false; isDecreasing = false; }
	
	/*for each input_rate_time_unit, the system capacity available is that amount of time
	 * multiplied by some headroom factor (<1)
	 */
	
	double systemCapacity = headroom_factor* input_rate_time_unit;

/********************************
 * DECIDE FOR DIFFERENCE CASES
 * *****************************/
	//bool delay_observed = (abnormal_response_time && (abnormal_cost<=0));					
		
	double excessload = 0;
	double k=0;

	//printf("initial gap: %d, nextgap: %d, critical_above_factor %f\n", initial_gap, next_gap, critical_above_factor);

	bool set_first_decreasing = false;
	bool set_first_increasing = false;


	if(systemCapacity < totalLoad)
	{
		
		/****
		 * if the target is really violated
		 ****/ 
		if(abnormal_response_time){

			//printf(" system capacity < total load - abnormal response time \n");

			int additional_drop;
			excessload = (totalLoad - systemCapacity)/totalLoad;
		
			//additional_drop = ceil((1-(((1-excessload)*totalLoad - totalSourceLoad)/(totalLoad - totalSourceLoad)))*100);
			//additional_drop = ceil((1-(((1-excessload)*totalLoad)/totalLoad))*100);
			additional_drop = excessload*100;
			additional_drop = (additional_drop >=1)? additional_drop : 1;
			addDrop(additional_drop);
			overloadded = true;
			
			if(additional_drop <= 5) //this is for keeping track of the effective cost
				this->numOfStableSheddingCycles++;
			else
				this->numOfStableSheddingCycles = 0;
				

		}
		else if(accumulated_delay)
		{
			/*accumulated delay observed, but the actual response time is less than the target
			 * we can allow more data if we are dropping something
			 * also, if we are dropping sth, it means the estimated capacity might be smaller than the actual one
			 * so, consider increasing it.
			 */
			
			if(overloadded){
				if(significantly_below_target && isDecreasing)
				{	
						remove_times ++;

						double max = totalLoad/(double)input_rate_time_unit;
					
						k = ((max-headroom_factor)/headroom_factor)*100;
						if(k<1) k=1;
					
						double factor = (log2(k+1)/(k));//*remove_times;

						headroom_factor += factor*(max - headroom_factor);
						remove_amount = 1 + round(log2(remove_times));

						if(removeDrop(remove_amount)==0)//no more overloadded
							 overloadded = false;

						is_first_increasing = false;
						set_first_increasing = true;

					//}
				}

			}

		}
		else {/*response time monitor says that it's normal state:
				*the capacity should be higher
				*/
			//printf(" system capacity < total load - normal state \n");
			double max = totalLoad/(double)input_rate_time_unit;	
			
			 k = ((max-headroom_factor)/headroom_factor)*100;
			 if(k<1) k=1;
			
			headroom_factor += (log2(k+1)/(k))*(max - headroom_factor);	
			
		}	
	}
	else{ // total load <= system capacity
		
		if(abnormal_response_time) { //delay target violated
			//printf(" system capacity >= total load - abnormal response time \n");
			//there is disagreement between the two component: consider decreasing the estimated capacity
			if(/*abnormal_cost <=0 && */critically_abnormal_response_time && isIncreasing)
			{
					add_times ++;

					double min = totalLoad/(double)input_rate_time_unit;
					/*this can be just a temporary increase, so decrease the headroom_factor by haft of the distance,
					*/
					k = ((headroom_factor-min)/headroom_factor)*100;
					if(k<1) k=1;

					double factor = (log2(k+1)/(k));//*add_times;
					//factor = (factor<1)? factor:1;

					headroom_factor -= factor*(headroom_factor - min);

					add_amount = 1+ round(log2(add_times));

					addDrop(add_amount);

					overloadded = true;
					//critical_above_factor += ((double)next_gap/100.0);
					
					is_first_decreasing = false;
					set_first_decreasing = true;
				//}
			}
		}
		else { //UT or normal
			
			if(overloadded){

				//printf(" system capacity >= total load - UT or normal \n");
				double freeload = 0;
				if(totalLoad >0){
					freeload = (systemCapacity - totalLoad)/totalLoad;
					//int removed_drop = floor(((((1+freeload)*totalLoad - totalSourceLoad)/(totalLoad - totalSourceLoad))-1)*100);
					//int removed_drop = round(((((1+freeload)*totalLoad)/totalLoad)-1)*100);
					int removed_drop = freeload*100;
					if (removed_drop<1) removed_drop = 1;
					if(removed_drop >0)
						if(removeDrop(removed_drop)==0)//no more overloadded
							 overloadded = false;
				
					if(removed_drop <= 5) //if the fluctation is small
						this->numOfStableSheddingCycles++;
					else
						this->numOfStableSheddingCycles = 0;
				}
			}
			
		}	
		
	}		
	
	
	if(!set_first_decreasing){
		is_first_decreasing = true;
		add_amount = 0;
		add_times = 0;
	}
	if(!set_first_increasing){
		is_first_increasing = true;
		remove_amount = 0;
		remove_times = 0;
	}
	if(recomputeLoadCoefficient && !still_abnormal)
			abnormal_cost  = (abnormal_cost<=0)? abnormal_cost:abnormal_cost-1;		
					
	//for experiments only: log to see how much drop is used
	if(sheddingLogFile){
		for(int i=0;i<numInputs;i++)
			if(inputs[i]->kind == PO_STREAM_SOURCE){
				int rate = round (((StreamSource*)inputs[i]->instOp)->inputRate);
				fprintf(sheddingLogFile, "%lld:%d:%d:%2.2f:%d:%d:%d:%d:%f:%f\n", 1000*((StreamSource*)inputs[i]->instOp)->getLastInputTs(),rate,((StreamSource*)inputs[i]->instOp)->drop_percent, headroom_factor, abnormal_response_time, critically_abnormal_response_time, accumulated_delay, significantly_below_target, critical_above_factor, critical_below_factor);
				break;
			}
	}		

	/*if(sheddingLogFile){
		int rate = round (((StreamSource*)inputs[0]->instOp)->inputRate);
		fprintf(sheddingLogFile, "%lld:%d:%d:%2.2f\n", 1000*((StreamSource*)inputs[0]->instOp)->getLastInputTs(),rate,((Drop *)drops[0]->instOp)->dropPercent, headroom_factor);
	}*/
	return 0;		
	 	
	
}

int LoadManager::findRelatedInputs_Drop(Physical::Operator *drop)
{
	for(unsigned int i=0;i<drop->numInputs;i++) //actually drop now has only one input
		findRelatedInput_Drop(drop->inputs[i],drop->instOp);
	return 0;
}
int LoadManager::findRelatedInput_Drop(Physical::Operator* op, Execution::Operator *drop)
{
	if(op->kind==PO_STREAM_SOURCE)
		((Drop*)drop)->addRelatedInput(op); //the method addRelatedInput includes eliminating duplicates
	else
		for(unsigned int i=0;i<op->numInputs;i++)
			findRelatedInput_Drop(op->inputs[i],drop);
	return 0;
		
}

int LoadManager::findRelatedOutputs_Drop(Physical::Operator* drop)
{
	for(int i=0;i< drop->numOutputs; i++)
		findRelatedOutput_Drop(drop->outputs[i],(Drop*)drop->instOp);
	return 0;
}
int LoadManager::findRelatedOutput_Drop(Physical::Operator* op, Drop *drop)
{
	if(op->kind==PO_OUTPUT)
		drop ->addRelatedOutput(op); //this checks for duplicate
	else
		for(unsigned int i=0; i<op->numOutputs;i++)
			findRelatedOutput_Drop(op->outputs[i],drop);
	return 0;
} 

int LoadManager::computeLoadCoefficient (Physical::Operator *op, double preSel, double preCoef, double source_rate, double& coef, double& cur_coef,double &effective_coef)
{

	coef = preSel*op->instOp->local_cost_per_tuple;
	cur_coef = preSel*op->instOp->snapshot_local_cost_per_tuple;
	preCoef += coef;
	if(effective_cost_available){
		//if(op->kind != PO_STREAM_SOURCE)
			effective_coef = preSel*op->instOp->effective_cost;
		//else
			//effective_coef = preSel*op->instOp->snapshot_local_cost_per_tuple;
	
	}
	
	if(op->kind != PO_OUTPUT){		
		double c, cur_c, effective_c;	
		for(unsigned int o =0; o< op->numOutputs; o++){
			if(op->outputs[o]->instOp->query_class_id == op->instOp->query_class_id){
				computeLoadCoefficient(op->outputs[o], preSel*op->instOp->local_selectivity, preCoef,source_rate, c, cur_c,effective_c);
				coef +=c;
				cur_coef +=cur_c;
				if(effective_cost_available)
					effective_coef +=effective_c;
			}
			else
			{
				ASSERT(op->outputs[o]->instOp->isShedder==true);
				op->outputs[o]->instOp->input_load += preSel*op->instOp->local_selectivity*source_rate;
			}
		}
	}
	else{
		op->u.OUTPUT.queryLoad += preCoef*source_rate;
	}

#ifdef _CTRL_LOAD_MANAGE_
		op->instOp->ctrl_load_coef = coef/preSel;
#endif //_CTRL_LOAD_MANAGE		
	
	return 0;
		
}
int LoadManager::addDrop(int decrease_percent)
{
	//now assume that every drops is added the same drop_percent
	if(decrease_percent>100) decrease_percent = 100;
	
	for(unsigned int i = 0;i<numInputs; i++)
	{
		
		Operator * source = inputs[i]->instOp;
		int old = source->drop_percent;
		source->drop_percent = round(100-((100-source->drop_percent)*(100-decrease_percent)/100.0));
		
		if (source->drop_percent<=old && old <95) source->drop_percent = old+1; 
		//printf("drop add: %d; drop: %d\n", decrease_percent, source->drop_percent);
		if(source->drop_percent >=95 ) source->drop_percent = 95;
	}
	
	/*for(unsigned int i = 0;i<numDrops; i++)
	{
		Drop * drop = (Drop*)drops[i]->instOp;
		drop->dropPercent = 100-floor((100-drop->dropPercent)*(100-decrease_percent))/100;
		//printf("\ndrop add: %d; drop: %d", decrease_percent, drop->dropPercent);
	}*/ 
	return 0;
}
int LoadManager::removeDrop(int increase_percent)
{
	
	bool overload = false;
	//now assume that every drops is removed the same drop_percent
	for(unsigned int i=0;i<numInputs; i++)
	{
		Operator* source = inputs[i]->instOp;
		int old = source->drop_percent;
		int new_drop_percent = round(100-(double)((100+increase_percent)*(100-source->drop_percent))/(double)100); 
		if(new_drop_percent <= 0)
			source->drop_percent = 0;
		else{
			
			if(new_drop_percent >= old && old>1)
				source->drop_percent = new_drop_percent-1;
			else
				source->drop_percent= new_drop_percent;	
			overload = true;
		}
		//printf("drop remove: %d; drop: %d\n", increase_percent, source->drop_percent);	
	}
	
	if(!overload) return 0; //no more shedding applied
	else return -1;  


}

int LoadManager::setHeavilyLoaddedCosts()
{
	/*Physical::Operator *op = usedOps;
	while(op){
	
		op->instOp->setHeavilyLoaddedCost();
		op = op->next;
	}*/
	
	for(int i=0;i<numOps;i++)
		ops[i]->instOp->setHeavilyLoaddedCost();
	return 0;
}

/*
 * PURDUE's CTRL-BASED LOAD SHEDDING
 */
 #ifdef _CTRL_LOAD_MANAGE_

double LoadManager::computeQueuingLoad()
{
	double qLoad;
	Physical::Operator* op;
	//op= usedOps;
	//while(op)
	for(int i=0;i<numOps;i++)
	{
		op = ops[i];
		//if(op->kind == PO_SELECT)
		//printf("type: %d, %f\n",op->kind, op->instOp-> ctrl_num_of_queuing_tuples);
		qLoad +=op->instOp-> ctrl_num_of_queuing_tuples*op->instOp->ctrl_load_coef;
		//op = op->next;
	}
	return qLoad;
}

int LoadManager::setDropPercent(int percent, unsigned long long int effective_time)
{
	for(unsigned int i = 0;i<numInputs; i++)
	{
		StreamSource * source = (StreamSource*)inputs[i]->instOp;
		
		source->push_drop_info((unsigned long long int)percent,effective_time);
		
		//source->drop_percent = (unsigned int)percent;
		//printf("effective time: %lld\n", effective_time);
	}
	
	//now assume that every drops is added the same drop_percent
	/*for(unsigned int i = 0;i<numDrops; i++)
	{
		Drop * drop = (Drop*)drops[i]->instOp;
		drop->dropPercent = percent;
		//printf("\ndrop add: %d", drop->dropPercent);
	}*/ 
	return 0;
}
#endif //_CTRL_LOAD_MANAGE_
 
void LoadManager::incoming_tuples_monitor()
{
	printf("\nreal: %f; estimated: %f\n", ((StreamSource*)inputs[0]->instOp)->ctrl_actual_total_incoming_tuples, ((StreamSource*)inputs[0]->instOp)->ctrl_estimated_total_incoming_tuples);
}
	

void LoadManager::resetCapacityUsageTracking()
{
	avg_capacity_usage = 0;
	cycle_count = 0;
}

void LoadManager::updateAvgCapacityUsage(double totalLoad)
{
	double current_load =  (totalLoad/(100-(inputs[0]->instOp)->drop_percent))*100;
	double current_usage = (current_load/(headroom_factor* input_rate_time_unit))*100;
	
	avg_capacity_usage = (avg_capacity_usage*cycle_count + current_usage)/(++cycle_count);
	//printf("%d: %f: %f\n", cycle_count, totalLoad, current_usage );
}
void LoadManager::updateAvgQueryLoad()
{
	//this method is called after each load management cycle, when the query load in that cycle
	//has been updated by the method computeLoadCoefficient
	for(int i =0;i<numOps;i++)
		if(ops[i]->kind == PO_OUTPUT){
			double currentQueryLoad =
					(ops[i]->u.OUTPUT.queryLoad/(100-(inputs[0]->instOp)->drop_percent))*100;
			currentQueryLoad = (currentQueryLoad/(headroom_factor* input_rate_time_unit))*100;
			ops[i]->u.OUTPUT.avgQueryLoad = (ops[i]->u.OUTPUT.avgQueryLoad
											*ops[i]->u.OUTPUT.cycleCount + currentQueryLoad)
											/(++ops[i]->u.OUTPUT.cycleCount);
			//reset the query load for the new load management cycle
			ops[i]->u.OUTPUT.queryLoad = 0;

		}
}

void LoadManager::findSource(Physical::Operator* op, set<Physical::Operator*> &relatedSource){
	if(op->kind==PO_STREAM_SOURCE){
		relatedSource.insert(op);
	}
	else
		for(unsigned int i=0;i<op->numInputs;i++)
			findSource(op->inputs[i],relatedSource);
}

void LoadManager::getSourceFilePos(std::set<int> queryIDs,std::map<Physical::Operator*,streampos> &sourceFilePos){
	set<Physical::Operator*> relatedSources;
	for (int i=0;i<numOutputs; i++){
		set<int>::iterator it = queryIDs.find(outputs[i]->u.OUTPUT.queryId);
		if(it!=queryIDs.end()){
			findSource(outputs[i],relatedSources);
		}
	}
	set<Physical::Operator*>::const_iterator it;
	for(it = relatedSources.begin(); it!=relatedSources.end(); it++){
		sourceFilePos.insert(std::pair<Physical::Operator*,streampos>(*it, ((StreamSource*)((*it)->instOp))->getCurPos()));
	}
}
Physical::Operator* LoadManager::getSourceFromID(int sourceID){
	for(int i=0;i<numInputs; i++)
		if(inputs[i]->id==sourceID)
			return inputs[i];
	return 0;
}
void LoadManager::onStartTimestampSet(Physical::Operator* op, set<int>QueryIDs){

	//TODO: this implementation is only correct if there is no sharing.
	//in case of sharing, operators need to be checked against queryIDs set so as not to activate the
	//query segment that does not belong to the query being activated.
	//Panos said we don't need syn at output
	/*if(op->kind == PO_OUTPUT){
		op->instOp->status = START_PENDING;
	}*/
	/*else if (op->kind ==PO_GROUP_AGGR){
		op->instOp->status = START_PREPARING; //group agg needs special treatment at the start
	}*/
	//else{
		if(op->kind!=PO_STREAM_SOURCE)
			op->instOp->status = ACTIVE;
		for(int o=0;o<op->numOutputs;o++){
			onStartTimestampSet(op->outputs[o],QueryIDs);
		}
	//}

}

void LoadManager::onSourceCompleted(int queryID){
	for(int i=0;i<numOutputs;i++){
		if(outputs[i]->u.OUTPUT.queryId == queryID)
			outputs[i]->instOp->setOperatorStatus(START_PREPARING);
	}
}
