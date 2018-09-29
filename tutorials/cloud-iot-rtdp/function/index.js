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
  
  var obj = JSON.parse(attrs);
  var keys = Object.keys(obj);
  var ndevice = obj[keys[5]];
  var ntemp = obj[keys[0];
  console.log('***************device name --> ' + ndevice);
  console.log('***************device temp --> ' + ntemp);
  
  const deviceProm = getDeviceBy(ndevice);
  deviceProm.then(devices => {
  const device = devices[0][0];
  controlDeviceTemperature(device, ntemp);
  });
  
  
  function getDeviceBy (deviceName) {
  const query = datastore
  .createQuery('device')
  .filter('name', '=', deviceName);
  return datastore.runQuery(query);
}
  function controlDeviceTemperature (device, tempMeasured) {
  if (tempMeasured > device.tempAlertThredshold) {
    console.error(new Error('Measured temperature of: ' + tempMeasured + ' exceeds alert thredshold: ' + device.tempAlertThredshold + ' for ' + device.name));
  }
}
}