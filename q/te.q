

// table events - observer for tables in root context

.te.startwatching:{[n;insf;upsf;setf;delf]
  if[not -11h=type n;'tablename];
  if[not type[get n] in 98 99h;'notatable];
  .te.priv.oemupsert[`.te.priv.observers;(n;.z.w;insf;upsf;setf;delf)];
  .te.priv.tablecount[n]:count data:get n;
  data }

.te.stopwatching:{[n]
  if[not -11h=type n;'tablename];
  if[not type[get n] in 98 99h;'notatable];
  delete from `.te.priv.observers where tn=n, hdl=.z.w;
 }

.te.priv.oeminsert:@[get;`.te.priv.oeminsert;{insert}]

.te.priv.oemupsert:@[get;`.te.priv.oemupsert;{upsert}]

.te.priv.zvs_is_suppressed:0b

.te.priv.suppress_zvs:{ .te.priv.zvs_is_suppressed:x; }

.te.priv.allow_zvs:{ not .te.priv.zvs_is_suppressed }

.te.priv.observers:([] tn:"S"$(); hdl:"I"$(); insf:(); upsf:(); setf:(); delf:())

.te.priv.tablecount:(1#`placeholder)!1#0Nj;

// remove observers on handle close
.z.pc:{[zpc;w]
  delete from `.te.priv.observers where hdl=.z.w;
  zpc[.z.w] }[@[get;`.z.pc;{{[h];}}]]

.z.vs:{[zvs;n;i]
  if[n in exec tn from .te.priv.observers;
    0N!-3!("zvs";n;i;.te.priv.allow_zvs[]);
    if[.te.priv.allow_zvs[];
      if[not type[@[get;n;()]] in 98 99h;:()];
      if[0h=type i;
        // unfortuanately comma-colon inserts/upserts get reported
        // to .z.vs as a set, so would be better to use upsert/insert instead
        // because now we have to either figure out what changed (costs more
        // for every set, but possibly send less) or push whole table
        // every time (costs less but send more, possibly unnecessarily)
        // for now, send more possibly unnecessarily
        if[isset:()~i;.te.priv.notifyset[n;get n]]; // set
        if[isdelete:0<count i;.te.priv.notifydelete[n;i]];     // delete
        if[not isset|isdelete;'unknownupdate]
      ];
      // TODO: finish with other callbacks
    ]
  ];
/  zvs[n;i] }[@[get;`.z.vs;{{[n;i];}}]]
  zvs[n;i] }[{[n;i];}]

.te.priv.notify:{[k;n;arg]
  0N!("notify";k);
   if[count o:select from .te.priv.observers where tn=n;
      // TODO: add throttling for slow readers
      {[k;n;arg;r] if[not null f:r k; neg[r`hdl] (f;n;arg); if[r`hdl;neg[r`hdl][]]]; }[k;n;arg] each o;
      .te.priv.tablecount[n]:count get n;
   ];
 }

.te.priv.notifyinsert:{[n;rows] .te.priv.notify[`insf;n;rows] };

.te.priv.notifyupsert:{[n;rows] .te.priv.notify[`upsf;n;rows] };

.te.priv.notifyset:   {[n;data] .te.priv.notify[`setf;n;data] };

.te.priv.notifydelete:{[n;wc] .te.priv.notify[`delf;n;wc] };


 .q.insert:{[t;v]
   if[isref:-11h=type t;
     .te.priv.suppress_zvs 1b;
     r:.[.te.priv.oeminsert;(t;v);{.te.priv.suppress_zv:0b;'x}]; // r is inserted row indices
     .te.priv.suppress_zvs 0b;
     .te.priv.notifyinsert[t;v];
   ];
   if[not isref;
      r:t,v
   ];                            // t is not a global so no notification
   r  }


.q.upsert:{[t;v]
  if[isref:-11h=type t;
    .te.priv.suppress_zvs 1b;
    if[count k:keys t;
       iskeyedtab:0b;
       if[99h=type v;iskeyedtab:98h=type value v];
       // TODO: why did i want to know this again?
       // Was it because I want to break up a partially new upsert somehow?
    ];
    r:.[.te.priv.oemupsert;(t;v);{.te.priv.suppress_zv:0b;'x}];
    .te.priv.suppress_zvs 0b;
    .te.priv.notifyupsert[t;v];
  ];
  if[not isref;.te.priv.oemupsert[t;v]];
 }

.te.priv.assert_last:{[s;n;arg]
  0N!s;
  if[not n~.te.priv.lasttest_n;'namemismatch];
  if[not arg~.te.priv.lasttest_arg;'argmismatch];
  .te.priv.resetstate[];
 }

.te.priv.resetstate:{[]
  `.te.priv.lasttest_n set ();
  `.te.priv.lasttest_arg set ();
 }

.te.priv.test:{[]
   `t set ([] a:`a`b; b:1 2);
  .te.priv.resetstate[];
  f:{[n;arg] 0N!"HERE"; `.te.priv.lasttest_n set n; `.te.priv.lasttest_arg set arg;};
  .te.startwatching[`t;f;f;f;f];
  insert[t;`a`b!(`c;3)];
  .te.priv.assert_last["insert nokey noref";();()]; // didn't use a reference
  insert[n:`t;a:`a`b!(`d;4)];
  .te.priv.assert_last["insert nokey ref";n;a];
  upsert[t;`a`b!(`e;4)];
  .te.priv.assert_last["upsert nokey noref";();()]; // didn't use a reference
  upsert[n:`t;a:`a`b!(`f;6)];
  .te.priv.assert_last["upsert nokey ref";n;a];
  `t set tmp:([] a:`c`d; b:10 20);
  .te.priv.assert_last["set nokey ref";`t;tmp];
  t,:a:`a`b!(`g;7);
  .te.priv.assert_last["comma nokey ref";`t;t];
  delete from `t where a=`g;
  .te.priv.assert_last["delete nokey ref";`t;enlist parse "a=`g"];

  `t set ([a:`a`b] b:1 2);
  .te.priv.resetstate[];
  /f:{[n;arg] 0N!"HERE"; `.te.priv.lasttest_n set n; `.te.priv.lasttest_arg set arg;};
  /.te.startwatching[`t;f;f;f;f];
  insert[t;`a xkey enlist `a`b!(`c;3)];
  .te.priv.assert_last["insert key noref";();()]; // didn't use a reference
  insert[n:`t;a:`a`b!(`d;4)];
  .te.priv.assert_last["insert key ref";n;a];
  upsert[t;`a`b!(`e;4)];
  .te.priv.assert_last["upsert key noref";();()]; // didn't use a reference
  upsert[n:`t;a:`a`b!(`f;6)];
  .te.priv.assert_last["upsert key ref";n;a];
  `t set tmp:([a:`c`d] b:10 20);
  .te.priv.assert_last["set key ref";`t;tmp];
  t,:a:`a`b!(`g;7);
  .te.priv.assert_last["comma key ref";`t;t];
  t,:a:flip `a`b!(`g`h;70 80);
  .te.priv.assert_last["comma key part new ref";`t;t];
  delete from `t where a=`g;
  .te.priv.assert_last["delete key ref";`t;enlist parse "a=`g"];

  upsert[`t;([a:`d`e] b:100 200)];

 }

// below here ignored
\

q)t:([] a:`a`b; b:1 2)
q)q).z.vs:{0N!(x;y;value x)}
q)insert[`t;`a`b!(`c;3)]
(`t;();+`a`b!(`a`b`c;1 2 3))
,2
q)upsert[`t;`a`b!(`d;4)]
(`t;();+`a`b!(`a`b`c`d;1 2 3 4))
`t
q)t,:`a`b!(`e;5)
(`t;();+`a`b!(`a`b`c`d`e;1 2 3 4 5))
q)delete from `t where a=2
'type
=
`a`b`c`d`e
2
q)delete from `t where a=`c
(`t;,(=;`a;,`c);+`a`b!(`a`b`d`e;1 2 4 5))
`t
q)delete from `t where b<3
(`t;,(<;`b;3);+`a`b!(`d`e;4 5))
`t
q)t
a b
---
d 4
e 5
q)`d _ t
'type
q)delete a from `t
(`t;();+(,`b)!,4 5)
`t
q)update a:`a`b`c i from `t
(`t;();+`b`a!(4 5;`a`b))
`t
