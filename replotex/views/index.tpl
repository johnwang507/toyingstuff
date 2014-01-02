<!DOCTYPE html>
<html><head>
<title></title>
<meta charset="utf-8" />
<!--[if lt IE 9]><script language="javascript" type="text/javascript" src="js/excanvas.min.js"></script><![endif]-->
<link href="jquery.jqplot.min.css" rel="stylesheet" />
<link href="wjq.css" rel="stylesheet" />
</head>
<body>
<div id='mainp'>
  <div id='leftp'>
    <h3>绘图指令</h3><p id='msg'>{{err}}</p>
    <form action="/" method="GET" onsubmit="return wjq.toSubmit();">
      <textarea id="cmd" oninput='return wjq.showDimHint(event)' placeholder="例: line on user of tracker at project:231 version:v1.5,v2.0">{{cmd_txt}}</textarea>
      <input id="q" name="q" type="hidden" value="" />
      <input id="go" type="submit" value="执行" /></form>
    <div id="czone"></div>
  </div>
  <div id='rightp'>
    <h3>可用维度</h3>
    <p id='hintdim'></p>
    <h3 id='hinth'><span>维度</span>的可选值</h3>
    <ul id='hintl'>请输入指令</ul>
  </div>
</div>
<script>
var DIMS = {{!dims}},
    CHART_TYPE = {{!charTypes}},
    valRgxs = {{!valRgxs}}, 
    jqdata = {{!jqdata}},
    enums = {{!enums}};
</script>
<script src="jquery-1.6.4.min.js"></script>
<script src="jqplot-1.0.4r1121.min.js"></script>
<script src="underscore-min.js"></script>
<script src="wjq.js"></script>
</body>
</html>