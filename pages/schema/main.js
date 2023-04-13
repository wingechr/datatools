"use strict";

const repo = "https://raw.githubusercontent.com/wingechr/datatools/main";
const defaultSchemaUrl = repo + "/data/tabular-data-resource.schema.json";

window.$ = require('jquery');
window.bs = require('bootstrap');

const JSONEditor = require("@json-editor/json-editor").JSONEditor;
// eslint-disable-next-line max-len
window.typeahead = require('../../node_modules/corejs-typeahead/dist/typeahead.jquery.js');
const Bloodhound = require('../../node_modules/corejs-typeahead/dist/bloodhound.js');


// eslint-disable-next-line no-unused-vars

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
  let jsonEditor = new JSONEditor(
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
        remove_empty_properties: true,
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
            // console.log(ed.input, ed.schema.options.autocomplete);


            $(ed.input).typeahead({
              hint: true,
              highlight: true,
              minLength: 1,
            },
            {
              source: new Bloodhound({
                datumTokenizer: Bloodhound.tokenizers.whitespace,
                queryTokenizer: Bloodhound.tokenizers.whitespace,
                // `states` is an array of state names defined in "The Basics"
                local: ed.schema.options.autocomplete,
              }),
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
  jsonEditor.on('ready', () => patchEditor(jsonEditor));

  /* also patch newlyadded rows */
  jsonEditor.on('addRow', patchEditor);

  window.jsonEditor= jsonEditor;
});

/* uploader */
const fileInput = document.getElementById('btn-upload-json');

fileInput.addEventListener('change', (event) => {
  const file = event.target.files[0];
  const reader = new FileReader();
  reader.addEventListener('load', (event) => {
    const jsonString = event.target.result;
    const data = JSON.parse(jsonString);
    console.log(data);
    window.jsonEditor.setValue(data);
  });
  reader.readAsText(file);
});

/* downloader */
const fileOutput = document.getElementById('btn-download-json');

/**
 *
 * @param {*} exportObj
 * @param {*} exportName
 */
function downloadObjectAsJson(exportObj, exportName) {
  const dataStr = encodeURIComponent(JSON.stringify(exportObj, null, 4));
  const dataStr2 = "data:text/json;charset=utf-8," + dataStr;
  const downloadAnchorNode = document.createElement('a');
  downloadAnchorNode.setAttribute("href", dataStr2);
  downloadAnchorNode.setAttribute("download", exportName + ".json");
  document.body.appendChild(downloadAnchorNode); // required for firefox
  downloadAnchorNode.click();
  downloadAnchorNode.remove();
}


fileOutput.addEventListener('click', (event) => {
  let data = window.jsonEditor.getValue();
  console.log(data);
  downloadObjectAsJson(data, "metadata");
});
