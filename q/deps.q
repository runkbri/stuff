\d .deps

// upstreams and downstream depedencies in a tree
// figure out if a node is "ready" based on whether
// or not its upstreams are done
//
// build tree below and set nodes to done to see what becomes ready
//  A   B
//   \ /
//    C
//    |
//    D
/

q).deps.add[`A;`$()];
q).deps.add[`B;`$()];
q).deps.add[`C;`A`B];
q).deps.add[`D;`C];
q).deps.tree`
level id islast
---------------
0     A  1
1     C  0
2     D  0
0     B  1
1     C  1
2     D  1

q).deps.setdone`A
`symbol$()
q).deps.setdone`B
,`C
q).deps.setdone`C
,`D
q).deps.setdone`D
`symbol$()

\

ids:()
upstreams:()
downstreams:()
done:()

init:{[]
  `ids set `$();
  `upstreams set (1#`placeholder)!enlist `$();
  `downstreams set (1#`placeholder)!enlist `$();
  `done set (1#`placeholder)!enlist 0b;
 }

 .deps.priv.isinit:@[get;`isinit;{0b}];
if [not .deps.priv.isinit;init[];.deps.priv.isinit:1b];

// update a node in the tree. state of id is set to not done
// id - node to update sym
// ups - upstream nodes syms
add:{[id;ups]
 if[not all ups in ids;'invalidupstream];
 if[id in alldownstreams each ups;`circulardependency];
 if[not id in ids;`ids set asc ids,id];
  ups,:();
  upstreams[id]:ups;
  if[count ups;
    downstreams[ups]:distinct each downstreams[ups],\:id
  ];
  done[id]:0b;
 }

// remove a node from tree
// id - node to remove sym
remove:{[id]
  if[id in ids;
    done _: id;
    downstreams[i]:except[;id] each upstreams[i:where (id in) each downstreams];
    downstreams _: id;
    upstreams[i]:except[;id] each upstreams[i:where (id in) each upstreams];
    upstreams _: id;
    `ids set `s#ids except id;
  ];
 }

// addate state to done
// id - node that is done sym
// returns list of nodes that became ready because of this
setdone:{[id]
  done[id]:1b;
  d:downstreams id;
  d where ready each d }

// update state to not done
// id - node to reset sym
// returns list of downstream nodes that were also reset because of this
reset:{[id]
  done[id,d:alldownstreams id]:0b;
  d }

// are all this node's upstreams done?
// id - node to check sym
ready:{[id] not[done id] and all done upstreams id }

// flattened list of all downstreams for a node
// id - node to get downstreams for sym
alldownstreams:{[id] 1_ { d:downstreams x; $[count d;raze x,.z.s each d;1#x] }[id]}

// table representing the downstream tree
// in depth first search order.
// ([] level; id; islast)
// level is tree depth
// id is node id
// islast indicates whether all upstreams are above it at this point in table
// rootid - node(s) to check or ` for all trees
tree:{[rootid]
  // no root id means all root jobs
  if[null rootid;
    rootid:(where 0=count each upstreams) except `placeholder;
  ];
  tree:([] level:"I"$(); id:`$());
  // depth first search
  dfs:{[t;i;k]
    if[k in ids;
       t:t,enlist[`level`id!(i;k)]
    ];
    $[not count d:downstreams[k];
      t;
      raze .z.s[t;i+1] each d
    ] };
  tree:raze dfs[tree;0] each rootid,();
  update islast:1b from tree where i=(last;i) fby id
 }

 // doesn't actually test anything
 // just sets up a simple tree
.deps.priv.test:{[]
   add[`A;`$()];
   add[`B;`$()];
   add[`C;`A`B];
   add[`D;`C];
  }

// doesn't work very well because doesn't know about order
// TODO: something more useful
 .deps.priv.stress:{[]
   n:100000;
   r:0;
   m:n;
   while[0<m:m div 10;r+:1];
   allids:`$"job",/:(neg[r]#) each (r#"0"),/:string 1+til n;
   v:distinct each allids,'enlist each (n?6)?\:allids;
   v[;1]:asc each v[;1];
   v:v iasc v[;1];
   .[add;;{0N!x;}] each v[;0],'(1_) each v;
  }
