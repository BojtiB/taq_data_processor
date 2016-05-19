/ TODO: NAGYOBB CHUNKOKBAN VALO BEOLVASAS
/ TODO: http://code.kx.com/wiki/Reference/xasc sort table on disk
/ TODO: az elején leellenőrzni, hogy az adott napra a mappa létezik-e

/ Global variable

/ TODO: Set divider if the count of the bytes changes
divider:100000000;

/ Methods

filterQuote:{[quote;idx]
	0!select sym:idx`sym,max bid%divider,min ask%divider,midquote:.5*((max bid%divider)+(min ask%divider)) by time from quote where ex="N" 
	};

filterTrade:{[trade;idx]
		data:select sym:idx`sym,time,price%divider,size,initiation:`none from trade where ex="N";
		quotePart:select from quote where date=idx`date,sym=idx`sym;
		err;
		ct:0;
		do[count data;
			data[ct]

			ct:ct+1
		]
	};

loadAndSaveData:{[fullIdx;widths;types;columns;file;rootPathSym;dataTypeSym;filter]
	c:0;
	x:0;
	sumWidths:sum widths;

	while[(count fullIdx)>c;
		idx:fullIdx[c];

		chunkrows:(idx`end)-(idx`beg);   /number of rows to process in each chunk
		
		c:c+1;
	   	
	   	data:flip columns!(types;widths)1:(file;x;chunkrows*sumWidths);

	   	data:filter[data;idx];
		
		dateSym:` $ string (idx`date);

		path:` sv (rootPathSym,dateSym,dataTypeSym,`); /sv: concat list element with /
		path upsert .Q.en[rootPathSym] data;
		x:x+chunkrows*sum widths]
	};

/----------------------------------------------------------
dest:`:e:/taq3
srcRoot:`:e:/q/data;

qcolumns:`time`bid`ask`s`bsize`asize`mode`ex`mmid;
qtypes:"vjjiiihcs";
qwidths:4 8 8 4 4 4 2 1 4;

tcolumns:`time`price`size`tseq`g127`corr`cond`ex
ttypes:"vjiihhsc";
twidths:4 8 4 4 2 2 4 1;


files: asc key srcRoot;
qbins: files where files like"Q*[0-9][A-Z].BIN";
qidxs: files where files like"Q*[0-9][A-Z].IDX";

tbins: files where files like"T*[0-9][A-Z].BIN";
tidxs: files where files like"T*[0-9][A-Z].IDX";

if[(count qbins)<>(count qidxs);' "Q idx and bin files count dont match!"];
if[(count tbins)<>(count tidxs);' "T idx and bin files count dont match!"];
if[(count qbins)<>(count tbins);' "T idx and bin files count dont match!"];



show "Now we will process Q bin, Q idx, T bin and T idx files. Count: ";
show 4*(count qbins);

cq:0;
do[count qbins;

	qfile:` sv (srcRoot,qbins[cq]);
	show qfile;

	qidx:flip `sym`date`beg`end!("siii";10 4 4 4) 1: ` sv (srcRoot,qidxs[cq]);
	qidx:select sym,"D"$ string date,beg,end from qidx;
	cq:cq+1;

	show .z.T;
/NEMMENT!!!!	loadAndSaveData[qidx;qwidths;qtypes;qcolumns;qfile;dest;`quote;filterQuote];
	show .z.T;]

load dest;

tq:0;	
do[count tbins;

	tfile:` sv (srcRoot,tbins[tq]);
	show tfile;

	tidx:flip `sym`date`beg`end!("siii";10 4 4 4) 1: ` sv (srcRoot,tidxs[tq]);
	tidx:select sym,"D"$ string date,beg,end from tidx;

	tq:tq+1;

	show .z.T;
	loadAndSaveData[tidx;twidths;ttypes;tcolumns;tfile;dest;`trade;filterTrade];
	show .z.T;]



