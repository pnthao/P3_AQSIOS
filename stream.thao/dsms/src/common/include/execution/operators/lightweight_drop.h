/**
 * @file         lightweight_drop.h
 * @date         Feb 21, 2009
 * @brief        a "lightweight" drop operator that does not allocate new space for output tuples
 * @author		 Thao Pham
 */ 

#ifndef _LIGHTWEIGHT_DROP_
#define _LIGHTWEIGHT_DROP_

#ifndef _DROP_
#include "execution/operators/drop.h"
#endif

#ifndef _QUEUE_
#include "execution/queues/queue.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

#ifndef _CPP_OSTREAM_
#include <ostream>
#endif

namespace Execution{
class LightWeightDrop : public Drop
{
private:
/// System log
	std::ostream &LOG;
public:
	LightWeightDrop(unsigned int id, std::ostream &LOG);
	virtual ~LightWeightDrop();
	//run for timeSlice (maximum number of tuples processed in a scheduling cycle)
	int run (TimeSlice timeSlice); 
};

}

#endif /*_LIGHTWEIGHT_DROP_*/
