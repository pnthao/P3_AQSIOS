//drop operator instantiation, by Thao Pham

/*@file: inst_drop.cc
 *@date: Feb 22, 2009
 *@bief: implementing PlanMamagerImpl::inst_drop method
 */

#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _DROP_
#include "execution/operators/drop.h"
#endif

#ifndef _LIGHTWEIGHT_DROP_
#include "execution/operators/lightweight_drop.h"
#endif

#ifndef _HEAVYWEIGHT_DROP_
#include "execution/operators/heavyweight_drop.h"
#endif

#ifndef _TUPLE_
#include "execution/internals/tuple.h"
#endif

#ifndef _STORE_ALLOC_
#include "execution/stores/store_alloc.h"
#endif

#ifndef _AEVAL_
#include "execution/internals/aeval.h"
#endif

using namespace Metadata;
using Execution::Drop;
using Execution::StorageAlloc;
using Execution::Tuple;
using Execution::EvalContext;
using Execution::AEval;
using Execution::AInstr;
using Execution::LightWeightDrop;
using Execution::HeavyWeightDrop;

static const unsigned int INPUT_ROLE = 2;
static const unsigned int OUTPUT_ROLE = 3;

static int getCopyEval (Physical::Operator *op, AEval *&eval);

int PlanManagerImpl::inst_drop (Physical::Operator *op)
{
	int rc;
	Drop *drop;
	StorageAlloc *store;	
	TupleLayout  *tupleLayout;
	EvalContext *evalContext;
	AEval *copyEval;
	
	ASSERT (op -> kind == PO_DROP);
	ASSERT (op -> instOp == 0);
	//heavyweight drop only
	if(op->u.DROP.kind==PO_HWDROP)
	{	
		evalContext = new EvalContext ();

		// evaluator that copies the input tuple to an output tuple
		if ((rc = getCopyEval (op, copyEval)) != 0)
			return rc;
		if ((rc = copyEval -> setEvalContext (evalContext)) != 0)
			return rc;
			
		// Tuple layout of the output tuples
		tupleLayout = new TupleLayout (op);	
		
		// Storeage allocator, in  the case it need to serve some op above, this need to be window store
		ASSERT (op -> store);
		
		ASSERT (op -> bStream && (op -> store -> kind == SIMPLE_STORE ||
								  op -> store -> kind == WIN_STORE));
		
		if (op -> store -> kind == SIMPLE_STORE) {
			if ((rc = inst_simple_store (op -> store, tupleLayout)) != 0)
				return rc;
			store = op -> store -> instStore;
		}
		else { //window store 
			if ((rc = inst_win_store (op -> store, tupleLayout)) != 0)
				return rc;
			store = op -> store -> instStore;
		}
	}
	
	// Create the Drop object
	
	if(op->u.DROP.kind==PO_LWDROP)
		drop = new LightWeightDrop(op -> id, LOG);
	else if (op->u.DROP.kind==PO_HWDROP)
		drop = new HeavyWeightDrop(op -> id, LOG);
		
	if ((rc = drop -> setDropPercent (op -> u.DROP.dropPercent)) != 0)
		return rc;
/*	if ((rc = drop -> activate(op -> u.DROP.active)) != 0)
		return rc;
		*/
	//again, heavyweight drop only
	if((op->u.DROP.kind==PO_HWDROP))
	{	
		if ((rc = ((HeavyWeightDrop*)drop) -> setOutStore (store)) != 0)
			return rc;	
	
		if((rc=((HeavyWeightDrop*)drop)->setEvalContext(evalContext))!=0)
			return rc;
			
		if((rc=((HeavyWeightDrop*)drop)->setCopyEval(copyEval))!=0)
			return rc;
		
		delete tupleLayout;
	}
	
	op -> instOp = drop;
	
	return 0;
}

static int getCopyEval (Physical::Operator *op, AEval *&eval)
{
	int rc;
	AInstr instr;
	TupleLayout *tupleLayout;
	
	eval = new AEval ();
	tupleLayout = new TupleLayout (op);

	for (unsigned int a = 0 ; a < op -> numAttrs ; a++) {

		switch (op -> attrTypes [a]) {
		case INT:   instr.op = Execution::INT_CPY; break;
		case FLOAT: instr.op = Execution::FLT_CPY; break;
		case CHAR:  instr.op = Execution::CHR_CPY; break;
		case BYTE:  instr.op = Execution::BYT_CPY; break;
			
#ifdef _DM_
		default:
			ASSERT (0);
			break;
#endif
		}
		
		// source
		instr.r1 = INPUT_ROLE;
		instr.c1 = tupleLayout -> getColumn (a);
		
		// dest
		instr.dr = OUTPUT_ROLE;
		instr.dc = tupleLayout -> getColumn (a);

		if ((rc = eval -> addInstr (instr)) != 0)
			return rc;
	}

	delete tupleLayout;
	
	return 0;
}