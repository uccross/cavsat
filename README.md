# CAvSAT

CAvSAT (Consistent Answering via Satisfiability) project aims to be a scalable and comprehensive Consistent Query Answering (CQA) system. It encodes the problem of computing consistent answers into variants of Boolean Satisfiability (SAT), and uses modern SAT solvers to compute consistent answers to the input query.

Current version contains the query pre-processor, SQL-rewritability, and the SAT-solving modules of CAvSAT (see architecture below). Current version suppoerts unions of Select-Project-Join (SPJ) queries over databases that are inconsistent with respect to a set of arbitrary denial constraints.

![alt text](https://users.soe.ucsc.edu/~akadixit/img/Comprehensive-system-diagram.png)

Please contact Akhil Dixit (akadixit at ucsc dot edu) for more details about CAvSAT.
