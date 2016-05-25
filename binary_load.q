/ TODO: NAGYOBB CHUNKOKBAN VALO BEOLVASAS

/ Global variable

/ TODO: Set divider if the count of the bytes changes
divider:100000000;

/ Methods
/ A quote adatokat filterezi le csak a New York-i tőzsdére valamint 
/ kiszámolja minden másodpercre a max bid-et és min ask-ot valamint ezeknek a midquote-ját
/ quote: a filterezendő adat
/ idx: az adat egységről információk (melyik nap történt date, mi a szimbólum sym)
filterQuote:{[quote;idx]
	0!select sym:idx`sym,max bid%divider,min ask%divider,midquote:.5*((max bid%divider)+(min ask%divider)) by time from quote where ex="N" 
	};

/ A trade adatokat filterezi, valamint végrehajtja a Lee-Ready algoritmus első lépését
/ TODO Lee-Ready befejez
/ trade: a filterezendő adat
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

/ Betölti, filterezi majd menti az adatot
/ fullIdx: A TAQ adatok mellett található .IDX-re végzödő fájl összes sora.
/ widths: A különböző oszlopok nagysága bájtban.
/ types: A különböző oszlopok adat típusai : http://code.kx.com/wiki/Reference/Datatypes
/ columns: A betöltött oszlopok neve.
/ file: A file neve amit betöltünk.
/ rootPathSym: A feldolgozott adatok mentésének a helye.
/ dataTypeSym: milyen típusú az adat (quote, trade)
/ filter: A filterező függvény.
loadAndSaveData:{[fullIdx;widths;types;columns;file;rootPathSym;dataTypeSym;filter]
	c:0;
	x:0;
    / Egy sor nagysága bájtban
	sumWidths:sum widths;
    
    / Részletekben való beolvasás a bináris fájlból. Mindig az adott dátumhoz és szimbólumhoz tartozó sorokat olvassuk be egyszerre
    / majd ezeket feldolgozzuk és utána olvasunk be ismét egy adat "chunk"-ot.
	while[(count fullIdx)>c;
		idx:fullIdx[c];
        
        / Hány sort fogunk beolvasni
		chunkrows:(idx`end)-(idx`beg);   /number of rows to process in each chunk
		
		c:c+1;
	   	
        / Bináris adat beolvasása és átalakítása a types típusúakra
	   	data:flip columns!(types;widths)1:(file;x;chunkrows*sumWidths);
        
        / Adatok filterezése
	   	data:filter[data;idx];
		
		dateSym:` $ string (idx`date);
        
        / Adatok splayed table-ként való mentése.
		path:` sv (rootPathSym,dateSym,dataTypeSym,`); /sv: concat list element with /
		path upsert .Q.en[rootPathSym] data;
		x:x+chunkrows*sum widths]
	};

/----------------------------------------------------------
/ A betöltött és filterezett fájlok mentésének helye
destStr:"e:/taq4";
dest:` $ (":",destStr);

/ A mappa ahol a TAQ BIN és IDX fájlok megtalálhatóak
srcRoot:`:e:/q/data;

/ Quote file oszlopainak nevei.
qcolumns:`time`bid`ask`s`bsize`asize`mode`ex`mmid;
/ Quote file oszlopainak adat típasai.
qtypes:"vjjiiihcs";
/ Quote fájl oszlopainak nagyságga bájtban
qwidths:4 8 8 4 4 4 2 1 4;

/ Trade file oszlopainak nevei.
tcolumns:`time`price`size`tseq`g127`corr`cond`ex
/ Trade file oszlopainak adat típusai.
ttypes:"vjiihhsc";
/ Trade file oszlopainak nagyságga bájtban
twidths:4 8 4 4 2 2 4 1;

/ srcRoot változónál magadott mappában lévő fájlok listája
files: asc key srcRoot;

/ A fájlok között lévő Quote bin-ek és idx-ek.
qbins: files where files like"Q*[0-9][A-Z].BIN";
qidxs: files where files like"Q*[0-9][A-Z].IDX";

/ A fájlok között lévő Trade bin-ek és idx-ek.
tbins: files where files like"T*[0-9][A-Z].BIN";
tidxs: files where files like"T*[0-9][A-Z].IDX";

/ Annak vizsgálata, hogy ugyanannyi idx és bin valamint quote és trade fájl van.
if[(count qbins)<>(count qidxs);' "Q idx and bin files count dont match!"];
if[(count tbins)<>(count tidxs);' "T idx and bin files count dont match!"];
if[(count qbins)<>(count tbins);' "T and Q bin files count dont match!"];

show "Now we will process Q bin, Q idx, T bin and T idx files. Count: ";
show 4*(count qbins);

/ Quote fájlok feldolgozása
cq:0;
do[count qbins;
    
	qfile:` sv (srcRoot,qbins[cq]);
	show qfile;
    
    / A quote idx fájlának betöltése
	qidx:flip `sym`date`beg`end!("siii";10 4 4 4) 1: ` sv (srcRoot,qidxs[cq]);
	qidx:select sym,"D"$ string date,beg,end from qidx;
	cq:cq+1;
    
    / Quote betöltése, filterezése és mentése
	show .z.T;
	loadAndSaveData[qidx;qwidths;qtypes;qcolumns;qfile;dest;`quote;filterQuote];
	show .z.T];

/ Sorting quote by sym
dirs:dirs:asc key dest;
datedirs:dirs where dirs like"[0-9][0-9][0-9][0-9].[0-1][0-9].[0-3][0-9]";

/ A dátum után sym alapján rendezi a quote táblákat.
cd:0;
do[count datedirs;
	ddir:` sv (dest,datedirs[cd],`quote);
	cd:cd+1;
	show ddir;
	`sym xasc ddir
	];

/ Betölti a quote táblákat, hogy azzal a trade-eknél tudjunk dolgozni
system ("l ",destStr);

ct:0;
/ Trade fájlok feldolgozása
do[count tbins;
        
	tfile:` sv (srcRoot,tbins[ct]);
	show tfile;
    
    / A trade idx fájlának betöltése
	tidx:flip `sym`date`beg`end!("siii";10 4 4 4) 1: ` sv (srcRoot,tidxs[ct]);
	tidx:select sym,"D"$ string date,beg,end from tidx;
    ct:ct+1;
    
    / Trade betöltése, filterezése és mentése
	show .z.T;
	loadAndSaveData[tidx;twidths;ttypes;tcolumns;tfile;dest;`trade;filterTrade];
	show .z.T];



