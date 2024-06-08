# DRT Prod

drtprod is a tool for manipulating drt clusters using roachprod,
allowing easy creating, destruction, controls and configurations of clusters.

Commands include:<br/>
<b>push-hosts</b>: write the ips and pgurl files for a cluster to a node/cluster<br/>
<b>dns</b>: update/create DNS entries in drt.crdb.io for a cluster<br/>
<b>create</b>: a wrapper for the 'roachprod' with predefined specs for named clusters<br/>
<b>*</b>: any other command is passed to roachprod, potentialy with flags addded
