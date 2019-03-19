package com.querypreprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.beans.Atom;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;
import com.beans.TRCQuery;
import com.beans.TRCQuery.Formula;
import com.beans.TRCQuery.Op;
import com.beans.TRCQuery.Q;
import com.beans.TRCQuery.Quantifier;
import com.beans.TRCQuery.TupleVar;
import com.beans.TRCQuery.VarAttrPair;

public class CertainRewriter {
	public static final String FREE_TUPLE = "freeTuple";
	public static final String FREE_TUPLE_ALIAS = "free_";
	public static final Map<Integer, String[]> FREE_VARS_MAP = new HashMap<Integer, String[]>();

	public String getCertainRewritingSQL(Query query, Schema schema) {
		FREE_VARS_MAP.clear();
		TRCQuery q = convertToTRC(query, schema);
		q = certainRewriting(q);
		q.print();
		q = removeUniversalQuantifiers(q);
		return q.printSQL();
	}

	public TRCQuery certainRewriting(TRCQuery originalQuery) {
		TRCQuery trcQuery = new TRCQuery();
		trcQuery.getFreeVars().addAll(originalQuery.getFreeVars());

		Quantifier quantifier = null;
		List<Quantifier> list = originalQuery.getQuantifiers();
		Formula f = null;
		for (int i = 0; i < list.size(); i++) {
			quantifier = list.get(i);
			TupleVar sTuple = trcQuery.new TupleVar(quantifier.getTupleVar().getRelation(), "s" + (i + 1));
			trcQuery.getQuantifiers().add(trcQuery.new Quantifier(Q.EXISTS, sTuple));
			trcQuery.getQuantifiers().add(trcQuery.new Quantifier(Q.FORALL, quantifier.getTupleVar()));
			Relation relation = quantifier.getTupleVar().getRelation();
			for (String key : relation.getKeyAttributesList()) {
				Formula f1 = trcQuery.new Formula(trcQuery.new VarAttrPair(sTuple, key),
						trcQuery.new VarAttrPair(quantifier.getTupleVar(), key), Op.EQUALS);
				f = ((f == null) ? f1 : trcQuery.new Formula(f, f1, Op.AND));
			}
		}
		// this is in fact (f = f -> originalFormula)
		f = trcQuery.new Formula(trcQuery.negateFormula(f), originalQuery.getFormula(), Op.OR);
		trcQuery.setFormula(f);
		// trcQuery.printSQL();
		return trcQuery;
	}

	public TRCQuery convertToTRC(Query query, Schema schema) {
		TRCQuery trcQuery = new TRCQuery();

		Map<String, List<VarAttrPair>> varAttributes = new HashMap<String, List<VarAttrPair>>();

		TupleVar freeTuple = trcQuery.new TupleVar(null, FREE_TUPLE);
		for (int i = 0; i < query.getAtoms().size(); i++) {
			Atom atom = query.getAtoms().get(i);
			TupleVar tupleVar = trcQuery.new TupleVar(schema.getRelationByName(atom.getName()), "r" + (i + 1));
			Quantifier quantifier = trcQuery.new Quantifier(Q.EXISTS, tupleVar);
			trcQuery.getQuantifiers().add(quantifier);

			for (int j = 0; j < atom.getVars().size(); j++) {
				String var = atom.getVars().get(j);
				if (!varAttributes.containsKey(var)) {
					varAttributes.put(var, new ArrayList<VarAttrPair>());
				}
				varAttributes.get(var).add(trcQuery.new VarAttrPair(tupleVar,
						schema.getRelationByName(atom.getName()).getAttributes().get(j)));
			}
		}
		int k = 1;
		for (String var : varAttributes.keySet()) {
			VarAttrPair leftSide = varAttributes.get(var).get(0);
			VarAttrPair rightSide;
			if (query.getFreeVars().contains(var)) {
				trcQuery.getFreeVars().add(varAttributes.get(var).get(0));
				// freeTuple.setVar(leftSide.getTupleVar().getVar());
				VarAttrPair freeVarAttrPair = trcQuery.new VarAttrPair(freeTuple, Integer.toString(k));
				String[] arr = { leftSide.getTupleVar().getRelation().getName(), leftSide.getAttribute() };
				FREE_VARS_MAP.put(k, arr);
				if (trcQuery.getFormula() == null) {
					trcQuery.setFormula(trcQuery.new Formula(leftSide, freeVarAttrPair, Op.EQUALS));
				} else {
					trcQuery.setFormula(trcQuery.new Formula(trcQuery.getFormula(),
							trcQuery.new Formula(leftSide, freeVarAttrPair, Op.EQUALS), Op.AND));
				}
				k++;
			}
			for (int i = 1; i < varAttributes.get(var).size(); i++) {
				rightSide = varAttributes.get(var).get(i);
				if (trcQuery.getFormula() == null) {
					trcQuery.setFormula(trcQuery.new Formula(leftSide, rightSide, Op.EQUALS));
				} else {
					trcQuery.setFormula(trcQuery.new Formula(trcQuery.getFormula(),
							trcQuery.new Formula(leftSide, rightSide, Op.EQUALS), Op.AND));
				}
			}
		}
		return trcQuery;
	}

	public TRCQuery removeUniversalQuantifiers(TRCQuery originalQuery) {
		TRCQuery trcQuery = originalQuery;
		List<Quantifier> list = trcQuery.getQuantifiers();
		for (int i = 0; i < list.size(); i++) {
			switch (list.get(i).getQuantification()) {
			case EXISTS:
				continue;
			case FORALL:
				trcQuery.getQuantifiers().get(i).setQuantification(Q.NOTEXISTS);
				if (i == list.size() - 1) {
					trcQuery.setFormula(trcQuery.negateFormula(trcQuery.getFormula()));
				} else {
					trcQuery.getQuantifiers().get(i + 1).setQuantification(
							negateQuantifier(trcQuery.getQuantifiers().get(i + 1).getQuantification()));
				}
				break;
			case NOTEXISTS:
				continue;
			case NOTFORALL:
				trcQuery.getQuantifiers().get(i).setQuantification(Q.EXISTS);
				if (i == list.size() - 1) {
					trcQuery.setFormula(trcQuery.negateFormula(trcQuery.getFormula()));
				} else {
					trcQuery.getQuantifiers().get(i + 1).setQuantification(
							negateQuantifier(trcQuery.getQuantifiers().get(i + 1).getQuantification()));
				}
				break;
			}
		}
		return trcQuery;
	}

	private Q negateQuantifier(Q quantifier) {
		switch (quantifier) {
		case EXISTS:
			return Q.NOTEXISTS;
		case NOTEXISTS:
			return Q.EXISTS;
		case FORALL:
			return Q.NOTFORALL;
		case NOTFORALL:
			return Q.FORALL;
		}
		return null;
	}
}