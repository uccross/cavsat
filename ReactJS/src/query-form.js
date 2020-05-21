import React, { Component } from "react";
import ConsistentAnswers from "./ConsistentAnswers";
import PotentialAnswers from "./PotentialAnswers";
import PreviewDatabase from "./PreviewDatabase";
import DataComplexity from "./DataComplexity";
import RunningTimeAnalysis from "./RunningTimeAnalysis";
import $ from "jquery";
import "./index.css";
import "./tabs.css";

class QueryForm extends Component {
  constructor(props) {
    super(props);
    this.state = {
      schemaName: "",
    };
  }

  componentDidMount() {
    const schemaName = localStorage.getItem("schemaName");
    if (schemaName === null || schemaName === "") return;
    const dropDown = document.getElementById("selectSchema");
    this.setState({
      schemaName: schemaName,
    });
    for (var i = 0; i < dropDown.options.length; i++) {
      if (dropDown.options[i].value === schemaName) {
        dropDown.options[i].selected = true;
        break;
      }
    }
  }

  async executePressed(event) {
    event.preventDefault();
    $('#nav-tabs a[href="#tab2"]').tab("show");
    this.setState({
      schemaName: document.getElementById("selectSchema").value,
      querySyntax: document.getElementById("querySyntax").value,
      queryLanguage: document.querySelector(
        'input[name="inlineRadioOptions"]:checked'
      ).value,
      consAnsPreview: -1,
      satModuleRuntimeAnalysis: undefined,
      conQuerRuntimeAnalysis: undefined,
      kwRuntimeAnalysis: undefined,
      queryAnalysis: undefined,
    });
    //await this.analyseQuery();
    var evalStrategies = document.getElementsByName("eval-strategy");
    for (var i = 0; i < evalStrategies.length; i++) {
      if (evalStrategies[i].checked) {
        switch (i) {
          case 0:
            await this.runSATModule();
            break;
          case 1:
            if (this.state.queryAnalysis.dataComplexity === 0)
              await this.runConQuerRewriting();
            break;
          case 2:
            if (this.state.queryAnalysis.dataComplexity < 2)
              await this.runKWRewriting();
            break;
          default:
            break;
        }
      }
    }
    this.computePotentialAnswers();
  }

  async delayed() {
    await fetch("api/delayed-response", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        dbEnv: this.props.dbEnv,
      }),
    });
  }

  async runSATModule() {
    const response = await fetch("api/run-sat-module", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        dbEnv: this.props.dbEnv,
        schemaName: document.getElementById("selectSchema").value,
        querySyntax: document.getElementById("querySyntax").value,
        queryLanguage: document.querySelector(
          'input[name="inlineRadioOptions"]:checked'
        ).value,
      }),
    });
    const responseBody = await response.json();
    this.setState({
      satModuleRuntimeAnalysis: responseBody.runningTimeAnalysis,
    });
    if (
      this.state.consAnsPreview === null ||
      this.state.consAnsPreview === undefined ||
      this.state.consAnsPreview === -1
    ) {
      this.setState({
        consAnsPreview: responseBody,
      });
    }
  }

  async runConQuerRewriting() {
    const response = await fetch("api/run-conquer-rewriting", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        dbEnv: this.props.dbEnv,
        schemaName: this.state.schemaName,
        conQuerSQLRewriting: this.state.queryAnalysis.conquerRewriting,
      }),
    });
    const responseBody = await response.json();
    this.setState({
      conQuerRuntimeAnalysis: responseBody.runningTimeAnalysis,
    });
    if (
      this.state.consAnsPreview === null ||
      this.state.consAnsPreview === undefined ||
      this.state.consAnsPreview === -1
    ) {
      this.setState({
        consAnsPreview: responseBody,
      });
    }
  }

  async runKWRewriting() {
    const response = await fetch("api/run-kw-rewriting", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        dbEnv: this.props.dbEnv,
        schemaName: this.state.schemaName,
        kwSQLRewriting: this.state.queryAnalysis.kwRewriting.sql,
      }),
    });
    const responseBody = await response.json();
    this.setState({
      kwRuntimeAnalysis: responseBody.runningTimeAnalysis,
    });
    if (
      this.state.consAnsPreview === null ||
      this.state.consAnsPreview === undefined ||
      this.state.consAnsPreview === -1
    ) {
      this.setState({
        consAnsPreview: responseBody,
      });
    }
  }

  async computePotentialAnswers() {
    const response = await fetch("api/compute-potential-answers", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        dbEnv: this.props.dbEnv,
        schemaName: document.getElementById("selectSchema").value,
        querySyntax: document.getElementById("querySyntax").value,
        queryLanguage: document.querySelector(
          'input[name="inlineRadioOptions"]:checked'
        ).value,
      }),
    });
    const responseBody = await response.json();
    this.setState({
      potRuntimeAnalysis: responseBody.runningTimeAnalysis,
    });
    this.setState({
      potAnsPreview: responseBody,
    });
  }

  async analyseQuery() {
    const response = await fetch("api/get-query-analysis", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        dbEnv: this.props.dbEnv,
        schemaName: document.getElementById("selectSchema").value,
        querySyntax: document.getElementById("querySyntax").value,
        queryLanguage: document.querySelector(
          'input[name="inlineRadioOptions"]:checked'
        ).value,
      }),
    });
    const responseBody = await response.json();
    this.setState({
      queryAnalysis: responseBody,
    });
  }

  render() {
    return (
      <div>
        <div className="sticky">
          <div className="row overflow-hidden m-0 pt-3 pb-0 px-1 bg-light">
            <div className="col-md-10">
              <h6>Select-Project-Join (SPJ) Query</h6>
            </div>
            <div className="col-md-2">
              <div className="form-check form-check-inline float-right">
                <input
                  className="form-check-input"
                  type="radio"
                  name="inlineRadioOptions"
                  id="inlineRadio2"
                  value="fol"
                />
                <label className="form-check-label" htmlFor="inlineRadio2">
                  First-order Logic
                </label>
              </div>
              <div className="form-check form-check-inline float-right">
                <input
                  className="form-check-input"
                  type="radio"
                  defaultChecked
                  name="inlineRadioOptions"
                  id="inlineRadio1"
                  value="sql"
                />
                <label className="form-check-label" htmlFor="inlineRadio1">
                  SQL
                </label>
              </div>
              <div className="form-check form-check-inline float-right">
                <h6>Query Syntax</h6>
              </div>
            </div>
          </div>
          <form>
            <div className="row overflow-hidden m-0 p-1 bg-light">
              <div className="col-md-12">
                <textarea
                  type="text"
                  spellCheck="false"
                  className="form-control"
                  id="querySyntax"
                  rows="10"
                />
              </div>
            </div>
            <div className="row overflow-hidden m-0 px-1 pt-1 pb-2 bg-light">
              <div className="col-md-12 p-0">
                <div className="form-check form-check-inline float-right">
                  <h6 className="mr-3">Evaluation Strategies</h6>
                  <input
                    className="form-check-input"
                    name="eval-strategy"
                    type="checkbox"
                    id="inlineCheckbox1"
                    value="1"
                    defaultChecked
                  />
                  <label
                    className="form-check-label mr-3"
                    htmlFor="inlineCheckbox1"
                  >
                    SAT/MaxSAT Solving
                  </label>

                  <input
                    className="form-check-input"
                    name="eval-strategy"
                    type="checkbox"
                    id="inlineCheckbox2"
                    value="2"
                  />
                  <label
                    className="form-check-label mr-3"
                    htmlFor="inlineCheckbox2"
                  >
                    ConQuer SQL Rewriting
                  </label>

                  <input
                    className="form-check-input"
                    name="eval-strategy"
                    type="checkbox"
                    id="inlineCheckbox3"
                    value="3"
                  />
                  <label
                    className="form-check-label mr-3"
                    htmlFor="inlineCheckbox3"
                  >
                    Koutris-Wijsen SQL Rewriting
                  </label>
                  <button
                    type="button"
                    className="btn btn-sm btn-success mr-1 px-3"
                    onClick={this.executePressed.bind(this)}
                  >
                    <i className="fas fa-play mr-2"> </i> Execute
                  </button>
                </div>
              </div>
            </div>
          </form>
          <hr className="mt-0 abc" />
        </div>
        <div className="row position-relative overflow-hidden mb-md-3 m-0">
          <div className="col-md-12">
            <div id="tabs">
              <div
                className="nav nav-tabs nav-fill"
                id="nav-tabs"
                role="tablist"
              >
                <a
                  className="nav-item nav-link"
                  id="nav-tab1"
                  data-toggle="tab"
                  href="#tab1"
                  role="tab"
                  aria-controls="nav-home"
                  aria-selected="true"
                >
                  Preview Schema and Raw Data
                </a>
                <a
                  className="nav-item nav-link active"
                  id="nav-tab2"
                  data-toggle="tab"
                  href="#tab2"
                  role="tab"
                  aria-controls="nav-home"
                  aria-selected="true"
                >
                  Consistent Answers
                </a>
                <a
                  className="nav-item nav-link"
                  id="nav-tab3"
                  data-toggle="tab"
                  href="#tab3"
                  role="tab"
                  aria-controls="nav-profile"
                  aria-selected="false"
                >
                  Potential Answers
                </a>
                <a
                  className="nav-item nav-link"
                  id="nav-tab4"
                  data-toggle="tab"
                  href="#tab4"
                  role="tab"
                  aria-controls="nav-profile"
                  aria-selected="false"
                >
                  Query Analysis
                </a>
                <a
                  className="nav-item nav-link"
                  id="nav-tab5"
                  data-toggle="tab"
                  href="#tab5"
                  role="tab"
                  aria-controls="nav-profile"
                  aria-selected="false"
                >
                  Running Time Analysis
                </a>
              </div>

              <div
                className="tab-content py-0 px-3 px-sm-0"
                id="nav-tabContent"
              >
                <div
                  className="tab-pane fade show"
                  id="tab1"
                  role="tabpanel"
                  aria-labelledby="tab1"
                >
                  <PreviewDatabase
                    databasePreview={this.props.databasePreview}
                  />
                </div>
                <div
                  className="tab-pane fade show active"
                  id="tab2"
                  role="tabpanel"
                  aria-labelledby="tab2"
                >
                  <ConsistentAnswers
                    consAnsPreview={this.state.consAnsPreview}
                  />
                </div>
                <div
                  className="tab-pane fade"
                  id="tab3"
                  role="tabpanel"
                  aria-labelledby="tab3"
                >
                  <PotentialAnswers potAnsPreview={this.state.potAnsPreview} />
                </div>
                <div
                  className="tab-pane fade"
                  id="tab4"
                  role="tabpanel"
                  aria-labelledby="tab4"
                >
                  <DataComplexity queryAnalysis={this.state.queryAnalysis} />
                </div>
                <div
                  className="tab-pane fade"
                  id="tab5"
                  role="tabpanel"
                  aria-labelledby="tab5"
                >
                  <RunningTimeAnalysis
                    consAnsPreview={this.state.consAnsPreview}
                    potAnsPreview={this.state.potAnsPreview}
                    satRuntimeAnalysis={this.state.satModuleRuntimeAnalysis}
                    conQuerRuntimeAnalysis={this.state.conQuerRuntimeAnalysis}
                    kwRuntimeAnalysis={this.state.kwRuntimeAnalysis}
                    potRuntimeAnalysis={this.state.potRuntimeAnalysis}
                  />
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default QueryForm;
