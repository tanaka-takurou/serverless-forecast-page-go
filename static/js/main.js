$(document).ready(function() {
  drawChart();
});

var clearChart = function() {
  window.lineChart.destroy();
};

var drawChart = function() {
  var ctx = document.getElementById("lineChart");
  window.lineChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [...Array(App.data.length)].map((_, i) => i),
      datasets: [
        {
          label: 'Value',
          data: App.data,
          borderColor: "rgba(210,210,210,1)",
          backgroundColor: "rgba(0,0,0,0)",
          pointBackgroundColor: generatePointBgcolor(App.data.length)
        }
      ],
    },
    options: {
      title: {
        display: false,
      },
      legend: {
        display: false
      },
      scales: {
        yAxes: [{
          ticks: {
            suggestedMax: Math.max(...App.data),
            suggestedMin: 0,
            stepSize: 0.1,
            callback: function(value, index, values){
              return  value
            }
          }
        }]
      },
    }
  });
};

var SubmitForm = function(action) {
  $(".submitbutton").addClass('disabled');
  $("#loader").addClass('active');
  var data = JSON.stringify(App.data);
  const data_ = {action, data};
  request(data_, (res)=>{
    App.pid = res.message;
    $("#result").text("Data-Import process. Please wait.");
    $("#info").removeClass("hidden").addClass("visible");
    App.progress = "checkimport";
    CheckProgress();
  }, (e)=>{
    console.log(e.responseJSON.message);
    $("#warning").text(e.responseJSON.message).removeClass("hidden").addClass("visible");
    $(".submitbutton").removeClass('disabled');
    $("#loader").removeClass('active');
  });
};

var CheckProgress = function(action) {
  var action = App.progress;
  var id = App.pid;
  if (!id) {
    $("#warning").text("Progress ID is Empty").removeClass("hidden").addClass("visible");
    return false;
  }
  const data = {action, id};
  request(data, (res)=>{
    if (res.message == "ACTIVE") {
      switch (App.progress){
      case "checkimport":
        App.progress = "checkpredictor";
        $("#result").text("Predictor process. Please wait.");
        CheckProgress();
        break;
      case "checkpredictor":
        App.progress = "checkforecast";
        $("#result").text("Forecast process. Please wait.");
        CheckProgress();
        break;
      case "checkforecast":
        App.progress = "checkexport";
        $("#result").text("Data-Export process. Please wait.");
        CheckProgress();
        break;
      case "checkexport":
        $("#result").text("Result will be shown. Please wait.");
        GetResult();
        break;
      }
    } else if (res.message.endsWith('FAILED')) {
      $("#warning").text("Error: " + App.progress + " Failed").removeClass("hidden").addClass("visible");
    } else {
      setTimeout(function() {
        CheckProgress();
      }, 300000);
    }
  }, (e)=>{
    console.log(e.responseJSON.message);
    $("#warning").text(e.responseJSON.message).removeClass("hidden").addClass("visible");
    $(".submitbutton").removeClass('disabled');
    $("#loader").removeClass('active');
  });
};

var GetResult = function() {
  var action  = "getresult";
  var id = App.pid;
  if (!id) {
    $("#warning").text("Progress ID is Empty").removeClass("hidden").addClass("visible");
    return false;
  }
  const data = {action, id};
  request(data, (res)=>{
    $(".submitbutton").removeClass('disabled');
    $("#loader").removeClass('active');
    App.resultRange = 10;
    try {
      resData = JSON.parse(res.message);
      App.data = App.data.concat(resData);
      clearChart();
      drawChart();
      $("#result").text("Result data is shown blue dot.");
    } catch(e) {
      $("#warning").text("Result data parse Error.").removeClass("hidden").addClass("visible");
    }
  }, (e)=>{
    console.log(e.responseJSON.message);
    $("#warning").text(e.responseJSON.message).removeClass("hidden").addClass("visible");
    $(".submitbutton").removeClass('disabled');
    $("#loader").removeClass('active');
  });
};

var request = function(data, callback, onerror) {
  $.ajax({
    type:          'POST',
    dataType:      'json',
    contentType:   'application/json',
    scriptCharset: 'utf-8',
    data:          JSON.stringify(data),
    url:           App.url
  })
  .done(function(res) {
    callback(res);
  })
  .fail(function(e) {
    onerror(e);
  });
};

var ChangeData = function() {
  const v = $("#sampledata").val();
  switch (parseInt(v, 10)){
  case 1:
    UpdateData(App.sin);
    break;
  case 2:
    UpdateData(App.cos);
    break;
  case 3:
    UpdateData(App.lin);
    break;
  }
};

var ChangeFile = function() {
  const file = $('#filedata').prop('files')[0];
  readTextFile(file).then(onLoad());
};

var readTextFile = function(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.readAsText(file);
    reader.onload = () => resolve(reader.result);
    reader.onerror = error => reject(error);
  });
};

var onLoad = function() {
  return function(v) {
    CheckData(v);
  }
};

var ChangeText = function() {
  const v = $('#textdata').val();
  CheckData(v);
};

var CheckData = function(dataString) {
  var data = []
  const mn = 30
  const mx = 100
  try {
    data = JSON.parse('[' + dataString + ']');
  } catch(e) {
    $("#warning").text("Parse Error.").removeClass("hidden").addClass("visible");
    return
  }
  if (data.length >= mn && data.length <= mx) {
    UpdateData(data);
  } else {
    str = mn + " or more."
    if (data.length > mx) {
      str = mx + " or less."
    }
    $("#warning").text("Data Size Error. Please fix Data size " + str).removeClass("hidden").addClass("visible");
  }
}

var UpdateData = function(data) {
  App.resultRange = 0;
  App.data = data;
  $("#warning").text("").removeClass("visible").addClass("hidden");
  clearChart();
  drawChart();
}

var generatePointBgcolor = function(length) {
  var bgColors = [];
  for(var i = 0, len = length; i < len; i++) {
    if(i >= length - App.resultRange) {
      bgColors.push("rgba(0,0,255,1)");
    } else {
      bgColors.push("rgba(255,0,0,1)");
    }
  }
  return bgColors;
};

var App = {
  data: [0, 0.062791, 0.125333, 0.187381, 0.24869, 0.309017, 0.368125, 0.425779, 0.481754, 0.535827, 0.587785, 0.637424, 0.684547, 0.728969, 0.770513, 0.809017, 0.844328, 0.876307, 0.904827, 0.929776, 0.951057, 0.968583, 0.982287, 0.992115, 0.998027, 1, 0.998027, 0.992115, 0.982287, 0.968583, 0.951057, 0.929776, 0.904827, 0.876307, 0.844328, 0.809017, 0.770513, 0.728969, 0.684547, 0.637424, 0.587785, 0.535827, 0.481754, 0.425779, 0.368125, 0.309017, 0.24869, 0.187381, 0.125333, 0.062791, 0, -0.062791, -0.125333, -0.187381, -0.24869, -0.309017, -0.368125, -0.425779, -0.481754, -0.535827, -0.587785, -0.637424, -0.684547, -0.728969, -0.770513, -0.809017, -0.844328, -0.876307, -0.904827, -0.929776, -0.951057, -0.968583, -0.982287, -0.992115, -0.998027, -1, -0.998027, -0.992115, -0.982287, -0.968583, -0.951057, -0.929776, -0.904827, -0.876307, -0.844328, -0.809017, -0.770513, -0.728969, -0.684547, -0.637424, -0.587785, -0.535827, -0.481754, -0.425779, -0.368125, -0.309017, -0.24869, -0.187381, -0.125333, -0.062791],
  sin: [0, 0.062791, 0.125333, 0.187381, 0.24869, 0.309017, 0.368125, 0.425779, 0.481754, 0.535827, 0.587785, 0.637424, 0.684547, 0.728969, 0.770513, 0.809017, 0.844328, 0.876307, 0.904827, 0.929776, 0.951057, 0.968583, 0.982287, 0.992115, 0.998027, 1, 0.998027, 0.992115, 0.982287, 0.968583, 0.951057, 0.929776, 0.904827, 0.876307, 0.844328, 0.809017, 0.770513, 0.728969, 0.684547, 0.637424, 0.587785, 0.535827, 0.481754, 0.425779, 0.368125, 0.309017, 0.24869, 0.187381, 0.125333, 0.062791, 0, -0.062791, -0.125333, -0.187381, -0.24869, -0.309017, -0.368125, -0.425779, -0.481754, -0.535827, -0.587785, -0.637424, -0.684547, -0.728969, -0.770513, -0.809017, -0.844328, -0.876307, -0.904827, -0.929776, -0.951057, -0.968583, -0.982287, -0.992115, -0.998027, -1, -0.998027, -0.992115, -0.982287, -0.968583, -0.951057, -0.929776, -0.904827, -0.876307, -0.844328, -0.809017, -0.770513, -0.728969, -0.684547, -0.637424, -0.587785, -0.535827, -0.481754, -0.425779, -0.368125, -0.309017, -0.24869, -0.187381, -0.125333, -0.062791],
  cos: [1, 0.998027, 0.992115, 0.982287, 0.968583, 0.951057, 0.929776, 0.904827, 0.876307, 0.844328, 0.809017, 0.770513, 0.728969, 0.684547, 0.637424, 0.587785, 0.535827, 0.481754, 0.425779, 0.368125, 0.309017, 0.24869, 0.187381, 0.125333, 0.062791, 0, -0.062791, -0.125333, -0.187381, -0.24869, -0.309017, -0.368125, -0.425779, -0.481754, -0.535827, -0.587785, -0.637424, -0.684547, -0.728969, -0.770513, -0.809017, -0.844328, -0.876307, -0.904827, -0.929776, -0.951057, -0.968583, -0.982287, -0.992115, -0.998027, -1, -0.998027, -0.992115, -0.982287, -0.968583, -0.951057, -0.929776, -0.904827, -0.876307, -0.844328, -0.809017, -0.770513, -0.728969, -0.684547, -0.637424, -0.587785, -0.535827, -0.481754, -0.425779, -0.368125, -0.309017, -0.24869, -0.187381, -0.125333, -0.062791, -0, 0.062791, 0.125333, 0.187381, 0.24869, 0.309017, 0.368125, 0.425779, 0.481754, 0.535827, 0.587785, 0.637424, 0.684547, 0.728969, 0.770513, 0.809017, 0.844328, 0.876307, 0.904827, 0.929776, 0.951057, 0.968583, 0.982287, 0.992115, 0.998027],
  lin: [0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29, 0.3, 0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39, 0.4, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49, 0.5, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.6, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69, 0.7, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.8, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89, 0.9, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99],
  resultRange: 0,
  pid: "",
  progress: "",
  url: location.origin + {{ .ApiPath }},
};
