/*-------------------------------------------------------------------------
 *
 * engine_vector_types.c
 *
 * Copyright (c) Hydra, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/lsyscache.h"
#include "nodes/bitmapset.h"

#include "engine/engine.h"
#include "engine/vectorization/engine_vector_types.h"

#include "engine/utils/listutils.h"

VectorColumn *
BuildVectorColumn(int16 columnDimension, int16 columnTypeLen, 
				  bool columnIsVal, uint64 *rowNumber)
{
	VectorColumn *vectorColumn;

	vectorColumn = palloc0(sizeof(VectorColumn));

	vectorColumn->dimension = 0;
	vectorColumn->value = palloc0(columnTypeLen * COLUMNAR_VECTOR_COLUMN_SIZE);
	vectorColumn->columnTypeLen = columnTypeLen;
	vectorColumn->columnIsVal = columnIsVal;
	vectorColumn->rowNumber = rowNumber;

	return vectorColumn;
}

TupleTableSlot * 
CreateVectorTupleTableSlot(TupleDesc tupleDesc)
{
	int						i;
	TupleTableSlot			*slot;
	VectorTupleTableSlot	*vectorTTS;
	VectorColumn			*vectorColumn;

	static TupleTableSlotOps tts_ops;
	tts_ops = TTSOpsVirtual;
	tts_ops.base_slot_size = sizeof(VectorTupleTableSlot);

	slot = MakeTupleTableSlot(CreateTupleDescCopy(tupleDesc), &tts_ops
#if PG_VERSION_NUM >= PG_VERSION_19
									  , 0
#endif
									  );

	TupleDesc slotTupleDesc  = slot->tts_tupleDescriptor;

	/* Vectorized TTS */
	vectorTTS = (VectorTupleTableSlot*) slot;
	
	/* All tuples should be skipped in initialization */
	memset(vectorTTS->keep, false, COLUMNAR_VECTOR_COLUMN_SIZE);

	for (i = 0; i < slotTupleDesc->natts; i++)
	{		
		Oid columnTypeOid = TupleDescAttr(slotTupleDesc, i)->atttypid;
		
		int16 columnTypeLen = get_typlen(columnTypeOid);

		int16 vectorColumnTypeLen = 
			columnTypeLen == -1 ?  sizeof(Datum) : columnTypeLen;

		/* 
		 * We consider that type is passed by val also for cases where we have 
		 * typlen == -1. This is because we use pointer to VARLEN type and don't
		 * construct our own object.
		*/
		bool vectorColumnIsVal = vectorColumnTypeLen <= sizeof(Datum);

		vectorColumn = BuildVectorColumn(COLUMNAR_VECTOR_COLUMN_SIZE,
										 vectorColumnTypeLen,
										 vectorColumnIsVal,
										 vectorTTS->rowNumber);

		vectorTTS->tts.tts_values[i] = PointerGetDatum(vectorColumn);
		vectorTTS->tts.tts_isnull[i] = false;
	}

	vectorTTS->tts.tts_nvalid = tupleDesc->natts;

	return slot;
}


void
ExtractTupleFromVectorSlot(TupleTableSlot *out, VectorTupleTableSlot *vectorSlot, 
						   int32 index, List *attrNeededList)
{
	int attno;
	int output_pos = 0;
	bool packedOutput =
		out->tts_tupleDescriptor->natts == list_length(attrNeededList);

	if (!packedOutput)
	{
		int i;

		for (i = 0; i < out->tts_tupleDescriptor->natts; i++)
		{
			out->tts_values[i] = (Datum) 0;
			out->tts_isnull[i] = true;
		}
	}

	/*
	 * attrNeededList holds 0-based table attnos sorted in ascending order
	 * (built by ColumnarAttrNeeded via bms_next_member).  The vectorSlot's
	 * tts_values[] are indexed by output position (0-based) — the i-th
	 * entry corresponds to the i-th member of attrNeededList.  Packed slots
	 * use the same output-position layout, while full-width executor slots
	 * must receive values at their original table attnos so ExecQual can
	 * resolve Vars with the relation's varattno numbering.
	 */
	foreach_int(attno, attrNeededList)
	{
		int out_idx;
		VectorColumn *column = (VectorColumn *) vectorSlot->tts.tts_values[output_pos];
		int8 *rawColumRawData = (int8*) column->value + column->columnTypeLen * index;

		out_idx = packedOutput ? output_pos : attno;
		out->tts_values[out_idx] = fetch_att(rawColumRawData, column->columnIsVal, column->columnTypeLen);
		out->tts_isnull[out_idx] = column->isnull[index];
		output_pos++;
	}

	ExecStoreVirtualTuple(out);
}

void
WriteTupleToVectorSlot(TupleTableSlot *in, VectorTupleTableSlot *vectorSlot, 
					   int32 index)
{
	TupleDesc tupDesc = in->tts_tupleDescriptor;

	int i;

	//vectorSlot->keep[index] = true;

	for (i = 0; i < tupDesc->natts; i++)
	{
		VectorColumn *column = (VectorColumn *) vectorSlot->tts.tts_values[i];

		if (!in->tts_isnull[i])
		{
			column->isnull[column->dimension] = false;

			if (column->columnIsVal)
			{
				int8 *writeColumnRowPosition = (int8 *) column->value + column->columnTypeLen * index;

				store_att_byval(writeColumnRowPosition, in->tts_values[i], column->columnTypeLen);
			}
			else
			{
				Pointer val = DatumGetPointer(in->tts_values[i]);

				Size data_length = VARSIZE_ANY(val);

				Datum *varLenTypeContainer = NULL;

				varLenTypeContainer = palloc0(sizeof(int8) * data_length);
				memcpy(varLenTypeContainer, val, data_length);

				*(Datum *) ((int8 *) column->value + column->columnTypeLen * index) = 
					PointerGetDatum(varLenTypeContainer);
			}
		}

		column->dimension++;
	}
}

void
CleanupVectorSlot(VectorTupleTableSlot *vectorSlot)
{
	TupleDesc tupDesc = vectorSlot->tts.tts_tupleDescriptor;

	int i;

	for (i = 0; i < tupDesc->natts; i++)
	{
		VectorColumn *column = (VectorColumn *) vectorSlot->tts.tts_values[i];
		memset(column->isnull, true, COLUMNAR_VECTOR_COLUMN_SIZE);
		column->dimension = 0;
	}
	
	memset(vectorSlot->keep, true, COLUMNAR_VECTOR_COLUMN_SIZE);
	vectorSlot->dimension = 0;
}
