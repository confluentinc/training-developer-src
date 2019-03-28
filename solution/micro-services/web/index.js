var map = L.map('map').setView([60.192059,24.945831], 13);

L.tileLayer('https://cdn.digitransit.fi/map/v1/{id}/{z}/{x}/{y}.png', {
  attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, ' +
    '<a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>',
  maxZoom: 19,
  id: 'hsl-map'}).addTo(map);

var markers = [];
var latlngStart = L.latLng(60.19904,24.94050);
addMarker(latlngStart);

var latlngStart2 = L.latLng(60.19904,24.95060);
addMarker(latlngStart2);

function addMarker(latlng){
  var marker = L.marker(latlng, { draggable: true}).addTo(map);
  getAddress(marker);
  marker.addEventListener('moveend', function(e) {
    getAddress(e.target);
  });
  markers.push(marker);
}

function getAddress(marker) {
  var latlng = marker.getLatLng();
  marker.draggable = false;
  fetch("https://api.digitransit.fi/geocoding/v1/reverse?point.lat="+latlng.lat+"&point.lon="+latlng.lng+"&size=1")
      .then(function(response) {
        return response.json();
      })
      .then(function(geojson) {
        marker.bindPopup(geojson.features[0].properties.label);
        marker.draggable = true;
      });
}