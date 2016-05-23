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
		quotePart:`time xdesc select from quote where date=idx`date,sym=idx`sym;

		ct:0;
		do[count data;
			midquote:(first select from quotePart where time<=((data[ct;`time])-00:00:05))`midquote;
			$[(data[ct;`price]>midquote) & (midquote<>0n);
				data[ct;`initiation]:`buyer;
				if[data[ct;`price]<midquote;
					data[ct;`initiation]:`seller]
			];

			ct:ct+1
		];
		data
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
destStr:"e:/taq4";
dest:` $ (":",destStr);
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
if[(count qbins)<>(count tbins);' "T and Q bin files count dont match!"];

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
	/loadAndSaveData[qidx;qwidths;qtypes;qcolumns;qfile;dest;`quote;filterQuote];
	show .z.T]

/ Sorting quote by sym
dirs:dirs:asc key dest;
datedirs:dirs where dirs like"[0-9][0-9][0-9][0-9].[0-1][0-9].[0-3][0-9]";

cd:0;
do[count datedirs;
	ddir:` sv (dest,datedirs[cd],`quote);
	cd:cd+1;
	show ddir;
	`sym xasc ddir
	];

system ("l ",destStr);

	
{[tbin]

	tfile:` sv (srcRoot,tbin);
	show tfile;

	tidx:flip `sym`date`beg`end!("siii";10 4 4 4) 1: ` sv (srcRoot,tidxs[ct]);
	tidx:select sym,"D"$ string date,beg,end from tidx;

	show .z.T;
	loadAndSaveData[tidx;twidths;ttypes;tcolumns;tfile;dest;`trade;filterTrade];
	show .z.T} peach tbins;



