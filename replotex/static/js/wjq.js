var wjq = function(){
// Some server generated variables accessible to this script
//             0id  1name_en    2enum?  3type  4comp?
// var DIMS = [[1, "author",     1,     0,     0],
//             [2, "created",    0,     1,     1],
//             [3, "start_date", 0,     1,     1]], 
//     CHART_TYPE = ["bd","burndown","line","bar","pie","stackline","stackbar"],
//     valRgxs = [["^\\d+$", 'parseInt'], ["^(\\d\\d){1,4}$", 'normalize_date'], ["^\\d*\\.?\\d*$", 'parseFloat']];

var chart_settings={
    bd:{
        plugins: ['dateAxisRenderer','canvasTextRenderer', 'canvasAxisTickRenderer'],
        settings:{
            stackSeries: true,
            seriesDefaults: {
                fill: true},
            legend: {
                show: true,
                placement: 'outsideGrid'},
            axes: {
                xaxis: {
                    renderer: $.jqplot.CanvasAxisTickRenderer,
                    tickOptions: {
                        angle: -90},
                    drawMajorGridlines: false}}}},
    line:{
      plugins:['canvasAxisLabelRenderer', 'categoryAxisRenderer'],
      settings:{}},
    bar:{
      plugins:['canvasAxisLabelRenderer', 'categoryAxisRenderer'],
      settings:{}},
    pie:{
      plugins:['canvasAxisLabelRenderer', 'categoryAxisRenderer'],
      settings:{}},
    stackline:{
      plugins:['canvasAxisLabelRenderer', 'categoryAxisRenderer'],
      settings:{}}, 
    stackbar:{
      plugins:['canvasAxisLabelRenderer', 'categoryAxisRenderer'],
      settings:{}}
  };

function draw_plot(plot_data){
  var chartype = $.trim($('textarea#cmd').val()).split(/\s+/)[0],
      cs = chart_settings[chartype],
      plot_setting = JSON.parse(JSON.stringify(cs.settings));//create a clone(in the so-called fastest way) of the setting since some of their properties may be changed. 
  _.each(cs.plugins,function(ele){
    $('body').append($(streplace('<script src="jqplot-plugins/jqplot.hhh.min.js">aaaa</script>', ele)));
  });
  // plot_setting.axes.xaxis.ticks = plot_data[0][1];
  // plot_setting.axes.xaxis.ticks = ['aaa','bbb'];
var plot = _.map(plot_data.slice(1),function(ser){
    return _.zip(_.map(plot_data[0][1],function(ele){
        return '20' + ele.slice(0,2)+'-' +　ele.slice(2,4) +'-'+ ele.slice(4);
    }), ser);
  });

  $.jqplot ('czone', plot, 
    {
            stackSeries: true,
            seriesDefaults: {
                rendererOptions: {
                    smooth: true,
                    animation: {
                        show: true
                    }
                }},
            seriesColors:['darkgray', 'lightgray', '#B00000', '#C00000', '#D00000', '#E00000', '#F00000'],
            series: 
                _.map(plot_data[0][3]) //todo: complete 

            [
                {
                    fill: false,
                    label: '2010'
                },
                {
                    label: '2011'
                }
            ],
            legend: {
                show: true,
                placement: 'outsideGrid'},
            axes: {
                xaxis: {
                    renderer: $.jqplot.DateAxisRenderer,
                    tickRenderer: $.jqplot.CanvasAxisTickRenderer,
                    tickOptions: {
                        formatString:'%m-%d',
                        angle: -60},
                    min:plot[0][0][0],
                    // max:plot[0].slice(-1)[0][0],
                    // tickInterval:'3 days',
                    pad:0,
                    drawMajorGridlines: true},
                // yaxis: {pad:0}
                }});
}

function streplace(){ // A simple string template proccessor which uses "hhh" as placeholder for replacement.
    if(arguments.length>1){
        var args = Array.prototype.slice.call(arguments).slice(1);
        return arguments[0].replace(/(hhh)/g,function(){return args.shift();});
    }
    return arguments.length?arguments[0]:''; }

var dsl_kws = ['on','of','if'],
    kw_rgx_grp = dsl_kws.join('|'),
    ch_rgx_grp = CHART_TYPE.join('|'),
    kw_rgx = streplace("\\s+(hhh)\\s+(((?!\\b(hhh)\\b).)+)", kw_rgx_grp, kw_rgx_grp),
    // rgx=/^(line|pie)(\s+(on|if)\s+(((?!\b(on|if)\b).)+))?(\s+(on|if)\s+(((?!\b(on|if)\b).)+))?$/i ,//sample rgx
    cmdRgx = new RegExp(streplace("^(hhh)(hhh)?(hhh)?(hhh)?$", ch_rgx_grp, kw_rgx, kw_rgx, kw_rgx),"i"),
    dimRgx = new RegExp(streplace('(hhh)(\\:[^,\\:]+(,[^,\\:]+)*)?$', _.zip.apply(_, DIMS)[1].join('|')), 'i'),
    masked_vals = {}, //mask quote string to ensure rgx match
    mholder="msk";

function buildQ(){
    var qdata={},
        input = $.trim($('textarea#cmd').val()); 
    var tmpstr = input.replace(/"[^"]*"/ig,function(v){//cleasing the input
                                var pholder = mholder + _.keys(masked_vals).length + mholder;
                                masked_vals[pholder] = v.slice(1,v.length-1);
                                return pholder;
                            }) //mask quoted values
                        .replace(/\s*(,|\:)\s*/ig,function(v){return $.trim(v);}) //remove extra spaces
                        .replace(/\s{2,}/ig,function(v){return " ";}); 
    var mchs = tmpstr.match(cmdRgx);
    if(!mchs){damn();}
    qdata.c= mchs[1].toLowerCase();// "mchs" is like [orginal-str, chart-type, kw1_whole, kw1, kw1_val, nonsense, undefined, kw2_whole, ...]
    qdata.c = (qdata.c=='burndown'?'bd':qdata.c);
    var dimparts = _.reduce(mchs.slice(2), 
                      function(memo, _, idx, lst){
                        if (idx%5==1){memo.push([lst[idx], lst[idx+1]])}
                        return memo;
                      }, []);// will be like [['on', 'author:2,3'], [undefined, undefined], [...]]
    _.each(dimparts, function(ele){
      var kw = ele[0], kwv=ele[1];
      if(kw){qdata[kw] = (kw=='if'?_.map(kwv.split(/\s+/),checkDim):checkDim(kwv));}
    });
    if(qdata.c!='bd'){
      _.each(['on','if'], function(ele){
        if(!(ele in qdata)){damn('Missing "hhh" part.', ele);}
      });
    }
    return qdata;}

function checkDim(dimpart){
    var mch = dimpart.match(dimRgx);
    if(!mch){damn('Invalid Dimension Syntax "hhh"',dimpart);}
    var dname = mch[1].toLowerCase(), //dim name, e.g., "user"
        ddef = _.find(DIMS,function(v){return v[1]==dname;}), //e.g., [1,"author",1,1,0]
        dimId = ddef[0],
        dimValRgx = new RegExp(valRgxs[ddef[4]][0], 'ig'),
        checkValPart = function(part){
            var tmp = masked_vals[part]?masked_vals[part]:part ,
                tmch = tmp.match(/^\s*([^~\s]+)\s*~\s*([^~\s]+)\s*$/i);
            return tmch? (ddef[5]? _.map(tmch.slice(1),checkVal)
                                   :damn('Range not allowed "hhh"',tmp))
                        :checkVal(tmp);},
        checkVal = function(v){
              if(!v.match(dimValRgx)){damn('"hhh" not valid for "hhh" as "hhh"',v, mch[1], dimValRgx);}
              return eval(valRgxs[ddef[4]][1])(v);
          },
        dimVals = mch[2] ? _.map(mch[2].slice(1).split(','),checkValPart) : null;
    return [dimId,dimVals];
}

function damn(){
    var val = streplace.apply(null,arguments);
    throw val?val:'Syntax Error';}

function pad(s,w,p){return (w <= s.length)? s:pad(p+s,w,p);}
function normalize_date(v){
    var tday = new Date(),
        tdaystr = tday.getFullYear() + pad((tday.getMonth()+1)+'',2,'0') + pad(tday.getDate()+'',2,'0'),
        vstr = pad(v,8,'#'),
        bits = _.map(_.zip(tdaystr,vstr),function(zv){return zv[1]=='#'?zv[0]:zv[1];});
        return bits.join('');}
function ajax_dimvals(did){
    var pprj_id = 0;
    if(did != 4){
      var rgx = /(?:sub)?project\s*\:\s*(\d+)/i ,
          mch = $('textarea#cmd').val().match(rgx);
      pprj_id = mch?mch[1]:0;
    }
    var enum_url = streplace('/enum/hhh/hhh', pprj_id, did);
    $.ajax(enum_url, {dataType:'json'})
        .done(function(jdata){
          var dim_name = _.find(DIMS, function(dim){return dim[0]==did;})[1],
              hint_ul = $('ul#hintl');
          $('h3#hinth > span').html(dim_name);
          if(jdata && jdata.length>0){
            hint_ul.html('');
            _.each(jdata, function(ele){
              var li = $('<li>'),
                  ale = [].concat(ele),
                  elefn = function(ele){};
              _.each(ale, function(v){
                li.append($('<span>' + v + '</span>'))
              });
              li.hide().appendTo(hint_ul).show('slow');
            });
          }else{
            hint_ul.html('No data');
          }})
        .fail(function(xhr, st, err){
          show_err(err);
        });}
function prompt_hint(dim_name){
    var findfn = function(dim){return dim[1]==dim_name;},
        dim = _.find(DIMS, findfn);
    if (dim) {
      show_err(); // Remove error message
      if(dim[2]){ajax_dimvals(dim[0]);}
    }else{
      show_err('Invalid Dimension: ' + dim_name);
    }}
function show_err(msg){
    if(msg){
      $('p#msg').html(msg).fadeIn().fadeOut().fadeIn();
    }else{
      $('p#msg').fadeOut();
    }}

function show_avail_dims(){
  $('p#hintdim').append(
  _.map(DIMS, function(ele){
    return streplace('<span><strong>hhh</strong>--hhh</span>', ele[1], ele[2]);
  }).join(','));
}

function page_ready(){
  show_avail_dims();
  var msg = $('p#msg');
  if(msg.html()){
    return msg.fadeIn().fadeOut().fadeIn();
  }
  return jqdata && draw_plot(jqdata);
}

$(page_ready);

return {
  'toSubmit':function(){
    try{
        $('input#q').val(JSON.stringify(buildQ()));
    }catch(e){
        show_err(e);
        return false;
    }},
  'showDimHint':function(event){
    var ele = event.target,
        caretPos = ele.selectionStart,
        dim_name = null;
    if(caretPos>0 && ele.value.charAt(caretPos-1)==':'){
        dim_name=ele.value.slice(0,caretPos).match(/\b([\w\-\.]+)\s*\:$/i)[1],
        prj_dim_nm = DIMS[3][1], // Have to hardcord "project" and "version" dimension here!
        ver_dim_nm = DIMS[9][1];
        if (ele.value.match(/^\s*(bd|burndown)\b/i) && [prj_dim_nm, ver_dim_nm].indexOf(dim_name)<0) {
          return show_err(streplace('"hhh" will be ignored. Only "hhh" and "hhh" are valid for a "Burndown".', dim_name, prj_dim_nm, ver_dim_nm));
        }
        prompt_hint(dim_name);
    }},
};}();
