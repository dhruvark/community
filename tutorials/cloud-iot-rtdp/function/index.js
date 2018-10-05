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

// [START functions_Datastore_setup]
const Datastore = require('@google-cloud/datastore');
// Instantiates a client
const datastore = Datastore();

// [START functions_pubsub_setup]
const PubSub = require('@google-cloud/pubsub');
// Instantiates a client
const pubsub = PubSub();
const topic = pubsub.topic('projects/iot-analytics-216917/topics/iotlab');
const publisher = topic.publisher();

exports.iot = function (event, callback) {
  const pubsubMessage = event.data;
  var attrs = Buffer.from(pubsubMessage.data, 'base64').toString().split(',');
  
 // 0: pressure, 1: temperature, 2: dewpoint, 3: device, 4: timestamp, 5: latitude, 6: longiture, 7: humidity
  var obj = JSON.parse(attrs);
  var keys = Object.keys(obj);
  var ndevice = obj[keys[3]];
  var ntemp = obj[keys[1]];
  var ndewpoint = obj[keys[2]];
  var nhumidity = obj[keys[7]];
  var npressure = obj[keys[0]];
 
 
  const deviceProm = getDeviceBy(ndevice);
  deviceProm.then(devices => {
  const device = devices[0][0];
  controlDeviceTemperature(device, ntemp);
  controlDeviceDeviceDewpoint(device, ndewpoint);
  controlDeviceDeviceHumidity(device, nhumidity);
  controlDeviceDevicePressure(device, npressure);
  });
  
  //const data = Buffer.from('Hello, world!');
  //publisher.publish(data);
  publishMessage(topicName, tosend);
  console.log('Sensor readings sent on pub-sub --> ' + attrs[0] + ', ' + attrs[1] + ', ' + attrs[2] + ', ' + attrs[3] + ', ' + attrs[4] + ', ' + attrs[5] + ', ' + attrs[6] + ', ' + attrs[7]);
  callback();
};
  
  const topicName = 'iotlab';
  const tosend = JSON.stringify({ foo: 'bar' });
  
  function publishMessage(topicName, tosend) {
  // [START pubsub_publish]
  // [START pubsub_quickstart_publisher]
  // Imports the Google Cloud client library
  const PubSub = require(`@google-cloud/pubsub`);

  // Creates a client
  const pubsub = new PubSub();

  /**
   * TODO(developer): Uncomment the following lines to run the sample.
   */


  //Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
  const dataBuffer = Buffer.from(tosend);

  pubsub
    .topic(topicName)
    .publisher()
    .publish(dataBuffer)
    .then(messageId => {
      console.log(`Message ${messageId} published.`);
    })
    .catch(err => {
      console.error('ERROR:', err);
    });
  // [END pubsub_publish]
  // [END pubsub_quickstart_publisher]
}


  function getDeviceBy (deviceName) {
  const query = datastore
  .createQuery('device')
  .filter('name', '=', deviceName);
  return datastore.runQuery(query);
}

  function controlDeviceTemperature (device, tempMeasured) {
  if (tempMeasured > device.tempAlertThredshold) {
    console.error(new Error(' ALERT! - Measured Temperature of: ' + tempMeasured + ' exceeds alert thredshold: ' + device.tempAlertThredshold + ' for ' + device.name));
  }}
  
  function controlDeviceDeviceDewpoint (device, dewpointmeasured) {
  if (dewpointmeasured > device.dpalertthreshold) {
    console.error(new Error(' ALERT! - Measured Dewpoint of: ' + dewpointmeasured + ' exceeds alert thredshold: ' + device.dpalertthreshold + ' for ' + device.name));
  }}
  
  function controlDeviceDeviceHumidity (device, humiditymeasured) {
  if (humiditymeasured > device.humidityAlertThreshold) {
    console.error(new Error(' ALERT! - Measured Humidity of: ' + humiditymeasured + ' exceeds alert thredshold: ' + device.humidityAlertThreshold + ' for ' + device.name));
  }}
  
  function controlDeviceDevicePressure (device, pressuremeasured) {
  if (pressuremeasured > device.pressAlertThreshold) {
    console.error(new Error(' ALERT! - Measured Pressure of: ' + pressuremeasured + ' exceeds alert thredshold: ' + device.pressAlertThreshold + ' for ' + device.name));
  }}