import React, { Component } from "react";
import BootstrapTable from "react-bootstrap-table-next";
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css";

class RunningTimeAnalysis extends Component {
  getComp(prop) {
    var Comp;
    if (prop === undefined) {
      Comp = (
        <div className="row">
          <div className="col-md-12">
            <p className="lead">-</p>
          </div>
        </div>
      );
    } else {
      Comp = (
        <div className="row">
          <div className="col-md-12">
            <BootstrapTable
              striped
              bordered={false}
              hover
              condensed
              bootstrap4={true}
              headerClasses="bg-secondary text-white"
              keyField={prop.name + "-data"}
              key={prop.name + "-data"}
              data={prop.data}
              columns={prop.columns}
            />
          </div>
        </div>
      );
    }
    return Comp;
  }
  render() {
    if (this.props.consAnsPreview === undefined) {
      return (
        <div>
          <p className="lead text-center mt-5 pt-5">
            Execute query to see running time analysis
          </p>
        </div>
      );
    } else if (this.props.consAnsPreview === -1) {
      return (
        <div>
          <p className="lead text-center mt-5 pt-5">
            Analysing running time...
          </p>
        </div>
      );
    } else {
      const satProp = this.props.satRuntimeAnalysis;
      const conquerProp = this.props.conQuerRuntimeAnalysis;
      const kwProp = this.props.kwRuntimeAnalysis;
      const potProp = this.props.potRuntimeAnalysis;
      var SATRuntimeAnalysis = this.getComp(satProp);
      var ConQuerRuntimeAnalysis = this.getComp(conquerProp);
      var KWRuntimeAnalysis = this.getComp(kwProp);
      var PotRuntimeAnalysis = this.getComp(potProp);

      return (
        <div className="container">
          <div>
            <div className="row">
              <div className="col-md-12 mt-4">
                <h5 className="p-0 mb-1">SAT / Partial MaxSAT Solving</h5>
              </div>
            </div>
            {SATRuntimeAnalysis}
          </div>
          <div>
            <div className="row">
              <div className="col-md-12 mt-4">
                <h5 className="p-0 mb-1">ConQuer SQL Rewriting</h5>
              </div>
            </div>
            {ConQuerRuntimeAnalysis}
          </div>
          <div>
            <div className="row">
              <div className="col-md-12 mt-4">
                <h5 className="p-0 mb-1">Koutris-Wijsen SQL Rewriting</h5>
              </div>
            </div>
            {KWRuntimeAnalysis}
          </div>
          <div>
            <div className="row">
              <div className="col-md-12 mt-4">
                <h5 className="p-0 mb-1">Potential Answers</h5>
              </div>
            </div>
            {PotRuntimeAnalysis}
          </div>
        </div>
      );
    }
  }
}

export default RunningTimeAnalysis;
