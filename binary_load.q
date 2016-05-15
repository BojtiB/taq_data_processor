
/ Methods
loadQuote:{do[(count qidx);
	idx:first select from qidx where i=c;
	chunkbeg:(idx`end);
	chunkrows:(idx`end)-(idx`beg);   /number of rows to process in each chunk
	c:c+1;

   	data:flip columns!(types;widths)1:(file;x;chunkrows*sumWidths);
   	data:select date:idx`date,sym:idx`sym,time,bid%divider,ask%divider,ex from data where ex="N"; /upsert to quote
	
	path: ` $ ("/" sv (":e:";"taq";"quote";string (idx`date);""));
   
	path upsert .Q.en[`:e:/taq] data;

	x:x+chunkrows*sum widths;]};

/----------------------------------------------------------

qidx:flip `sym`date`beg`end!("siii";10 4 4 4) 1: `:data/Q200405A.IDX;
qidx:select sym,"D"$ string date,beg,end from qidx;

file:`:data/Q200405A.BIN;

columns:`time`bid`ask`s`bsize`asize`mode`ex`mmid;

types:"vjjiiihcs";
widths:4 8 8 4 4 4 2 1 4;
sumWidths:sum widths;
divider:100000000;

c:0;
x:0;

