// Draw every embedded figure (M5-S2): each <script type="application/json" data-fig="id">
// holds a Plotly figure for the div #fig-id. Responsive, no Plotly logo, no network.
document.querySelectorAll('script[data-fig]').forEach(function (node) {
  var figure = JSON.parse(node.textContent);
  Plotly.newPlot('fig-' + node.dataset.fig, figure.data, figure.layout,
                 {responsive: true, displaylogo: false});
});
