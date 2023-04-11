"use strict";

require("bootstrap");

// TODO: $refs dont work?

// eslint-disable-next-line max-len
const defaultSchemaUrl = "https://raw.githubusercontent.com/wingechr/dataschema/master/dataschema/data/tabular-data-resource.schema.json";
const JSONEditor = require("@json-editor/json-editor").JSONEditor;
// eslint-disable-next-line no-unused-vars
let editor;


/**
 *
 * @param {str} url
 * @returns {Promise}
 */
function getJson(url) {
  return new Promise(function(resolve, reject) {
    const request = new XMLHttpRequest();
    request.open("GET", url);
    request.send();
    request.onload = function() {
      resolve(JSON.parse(request.responseText));
    };
  });
}

// update / fix query params
const urlSearchParams = new URLSearchParams(window.location.search);
let params = Object.fromEntries(urlSearchParams.entries());
params = {
  schema: params.schema || defaultSchemaUrl,
};
let url = (
  window.location.href.split("?")[0] + '?' +
  Object.keys(params).map((k) => k + "=" + params[k]).join("&")
);
history.pushState(undefined, undefined, url);
console.log(window.location.href);

// start editor
getJson(params.schema).then(function(schema) {
  editor = new JSONEditor(
      document.getElementById('editor'),
      {
        schema: schema,
        theme: "bootstrap5",
      },
  );
});
