#ifndef _LIN_STORE_
#define _LIN_STORE_

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif
#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

namespace Execution {
	class LinStore {
	public:
		
		/**
		 * Insert a tuple into the lineage synopsis stubId
		 */ 		
		virtual int insertTuple_l (Tuple tuple, Tuple *lineage,
								   unsigned int stubId) = 0;
		
		/**
		 * Delete a tuple from the lineage synopsis stubId
		 */
		
		virtual int deleteTuple_l (Tuple tuple, unsigned int stubId) = 0;
		
		/**
		 * Get the tuple with the specified lineage for the synopsis
		 * stubId
		 */		
		
		virtual int getTuple_l (Tuple *lineage, Tuple &tuple, 
								unsigned int stubId) = 0;
		//armaDILoS, by Thao Pham
		virtual void clearStore(unsigned int stubID) = 0;
		virtual void clearStore(unsigned int stubID, StorageAlloc* tupleStore) =0;
		//end of ArmaDILoS, by Thao Pham
		
	};
}

#endif
