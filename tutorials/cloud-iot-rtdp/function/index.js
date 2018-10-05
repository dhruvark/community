/*
# Copyright Google Inc. 2017
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
*/
const Datastore = require('@google-cloud/datastore');
// Instantiates a client
const datastore = Datastore();

exports.iot = function (event, callback) {
  const pubsubMessage = event.data;
  var attrs = Buffer.from(pubsubMessage.data, 'base64').toString().split(',');
  
 //0: temperature, 1: dewpoint, 2: timestamp, 3: humidity, 4: pressure, 5: metrocode, 6: device
  var obj = JSON.parse(attrs);
  var keys = Object.keys(obj);
  var ndevice = obj[keys[6]];
  var ntemp = obj[keys[0]];
  var ndewpoint = obj[keys[1]];
  var nhumidity = obj[keys[3]];
  var npressure = obj[keys[4]];
 
 
  const deviceProm = getDeviceBy(ndevice);
  deviceProm.then(devices => {
  const device = devices[0][0];
  controlDeviceTemperature(device, ntemp);
  controlDeviceDeviceDewpoint(device, ndewpoint);
  controlDeviceDeviceHumidity(device, nhumidity);
  controlDeviceDevicePressure(device, npressure);
  });
  
  console.log('Sensor readings sent on pub-sub --> ' + attrs[0] + ', ' + attrs[1] + ', ' + attrs[2] + ', ' + attrs[3] + ', ' + attrs[4] + ', ' + attrs[5] + ', ' + attrs[6]);
  callback();
};
  
 
  function getDeviceBy (deviceName) {
  const query = datastore
  .createQuery('device')
  .filter('name', '=', deviceName);
  return datastore.runQuery(query);
}

  function controlDeviceTemperature (device, tempMeasured) {
  if (tempMeasured > device.max_temp) {
    console.error(new Error(' !ALERT --> ' + device.name + ' <-- has recorded temperature above the max. threshold ' + device.max_temp + ' . Current temperature is: ' + tempMeasured));
  }
  else if (tempMeasured < device.min_temp) {
    console.error(new Error(' !ALERT --> ' + device.name + ' <-- has recorded temperature below the min. threshold ' + device.min_temp + ' . Current temperature is: ' + tempMeasured));
  else {
	console.log(' ' + device.name + ' is recording temperature between the optimal range of 70F to 73F. Current temperature is: ' + tempMeasured);
  }}
  
  
  function controlDeviceDeviceDewpoint (device, dewpointmeasured) {
  if (dewpointmeasured > device.max_dewpoint) {
    console.error(new Error(' ALERT! - Measured Dewpoint of: ' + dewpointmeasured + ' exceeds alert thredshold: ' + device.max_dewpoint + ' for ' + device.name));
  }}
  
  function controlDeviceDeviceHumidity (device, humiditymeasured) {
  if (humiditymeasured > device.max_humidty) {
    console.error(new Error(' ALERT! - Measured Humidity of: ' + humiditymeasured + ' exceeds alert thredshold: ' + device.max_humidty + ' for ' + device.name));
  }}
  
  function controlDeviceDevicePressure (device, max_pressure) {
  if (pressuremeasured > device.pressAlertThreshold) {
    console.error(new Error(' ALERT! - Measured Pressure of: ' + pressuremeasured + ' exceeds alert thredshold: ' + device.max_pressure + ' for ' + device.name));
  }}