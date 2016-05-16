/ TODO: NAGYOBB CHUNKOKBAN VALO BEOLVASAS
/ TODO: http://code.kx.com/wiki/Reference/xasc sort table on disk
/ Methods
filterQuote:{[quote;idx]
	divider:100000000;
	select sym:idx`sym,time,bid%divider,ask%divider,ex from quote where ex="N"};

loadAndSaveData:{[fullIdx;widths;types;columns;file;rootPathSym;dataTypeSym;filter]c:0;
	x:0;
	sumWidths:sum widths;

	do[(count fullIdx);
		idx:first select from fullIdx where i=c;
		
		chunkbeg:(idx`end);
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

qcolumns:`time`bid`ask`s`bsize`asize`mode`ex`mmid;

qtypes:"vjjiiihcs";
qwidths:4 8 8 4 4 4 2 1 4;
dest:`:e:/taq


srcRoot:`:data;

files: key srcRoot;
qbins: files where files like"Q*[0-9][A-Z].BIN";
qidxs: files where files like"Q*[0-9][A-Z].IDX";
tbins: files where files like"T*[0-9][A-Z].IDX";
tidxs: files where files like"T*[0-9][A-Z].IDX";

if[(count qbins)<>(count qidxs);' "Q idx and bin files count dont match!"];
if[(count tbins)<>(count tidxs);' "T idx and bin files count dont match!"];

cq:0;
do[count qbins;

	qfile:` sv (srcRoot,qbins[cq])
	show qfile;

	qidx:flip `sym`date`beg`end!("siii";10 4 4 4) 1: ` sv (srcRoot,qidxs[cq])
	qidx:select sym,"D"$ string date,beg,end from qidx;
	cq:cq+1;

	show .z.T;
	loadAndSaveData[qidx;qwidths;qtypes;qcolumns;qfile;dest;`quote;filterQuote];
	show .z.T;]

