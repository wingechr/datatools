"use strict";

import 'bootstrap';


// TODO: $refs dont work?

const repo = "https://raw.githubusercontent.com/wingechr/datatools/main";
const defaultSchemaUrl = repo + "/data/tabular-data-resource.schema.json";
const JSONEditor = require("@json-editor/json-editor").JSONEditor;


// eslint-disable-next-line no-unused-vars
let editor;


JSONEditor.defaults.callbacks = {
  "autocomplete": {

    // Setup API calls
    "search_za": function search(jseditor_editor, input) {
      return new Promise(function(resolve) {
        if (input.length < 2) {
          return resolve([]);
        }

        resolve(["aaaaa", "ababa"]);
      });
    },

    "renderResult_za": function(jseditor_editor, result, props) {
      return ['<li ' + props + '>',
        '<div class="eiao-object-title">' + result.data_json + '</div>',
        '<div class="eiao-object-snippet">' + result.uuid.substring(0, 7) + ' <small>' + result.schema_uuid.substring(0, 5) + '<small></div>',
        '</li>'].join('');
    },

    "getResultValue_za": function getResultValue(jseditor_editor, result) {
      return result.uuid;
    },
  },
};

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
        disable_collapse: true,
        disable_edit_json: true,
        disable_properties: true,
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

  /*
  editor.on('ready', () => {
    let ta = typeahead(document.getElementById("root[name]"), {
      source: ['foossss', 'barasdt', 'baz123'],
    });
  });
  */

  editor.watch('root.name', (x) => {
    console.log("ss", x);
  });
});
