"use strict";

import 'bootstrap';


// TODO: $refs dont work?

const repo = "https://raw.githubusercontent.com/wingechr/datatools/main";
const defaultSchemaUrl = repo + "/data/tabular-data-resource.schema.json";
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
      // https://github.com/json-editor/json-editor
      {
        schema: schema,
        theme: "bootstrap5",
        iconlib: "fontawesome5",
        remove_button_labels: true,
        compact: true,
        disable_collapse: false,
        disable_edit_json: true,
        disable_properties: false,
        array_controls_top: true,
        disable_array_reorder: true,
        disable_array_delete_last_row: true,
        disable_array_delete_all_rows: true,
        prompt_before_delete: false,
        show_opt_in: false,
        max_depth: 10, /* recursive depth */
        /* urn_resolver: null */
      },
  );
});
