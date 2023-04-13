"use strict";

import 'bootstrap';


// TODO: $refs dont work?

const repo = "https://raw.githubusercontent.com/wingechr/datatools/main";
const defaultSchemaUrl = repo + "/data/tabular-data-resource.schema.json";
const JSONEditor = require("@json-editor/json-editor").JSONEditor;
const typeahead = require('typeahead');

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
// console.log(window.location.href);

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

  /**
   * add autocomplete to all elements that have it
      if set placeholder, if exist
   * @param {object} editor
   */
  function patchEditor(editor) {
    // iterate over all nested sub-editors
    for (let key in editor.editors) {
      if (Object.hasOwn(editor.editors, key)) {
        let ed = editor.editors[key];
        if (ed) {
          if (ed.schema.options && ed.schema.options.autocomplete) {
            typeahead(ed.input, {
              source: ed.schema.options.autocomplete,
            });
          }

          // examples => placeholder
          if (ed.schema.options && ed.schema.options.placeholder) {
            ed.input.placeholder = ed.schema.options.placeholder;
          }
        }
      }
    }
  }

  /* patch initial */
  editor.on('ready', () => patchEditor(editor));

  /* also patch newlyadded rows */
  editor.on('addRow', patchEditor);
});
