import React, { Component } from "react";
import BootstrapTable from "react-bootstrap-table-next";
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css";
import "./index.css";

class ConsistentAnswers extends Component {
  render() {
    if (this.props.consAnsPreview === undefined) {
      return (
        <div>
          <p className="lead text-center mt-5 pt-5">
            Execute query to preview consistent answers
          </p>
        </div>
      );
    } else if (this.props.consAnsPreview === -1) {
      return (
        <div>
          <div className="loader mt-5 pt-5"></div>
          <p className="lead text-center mt-1 pt-1">
            Computing consistent answers ...
          </p>
        </div>
      );
    } else {
      const tableDetail = this.props.consAnsPreview.jsonDataPreview;
      return (
        <div>
          <div className="row">
            <div className="col-md-6 mt-4">
              <h5 className="p-0 mb-1">
                Result via {this.props.consAnsPreview.approach}
              </h5>
            </div>
            <div className="col-md-6 mt-4">
              <p className="p-0 mb-1 text-right">
                (Showing {this.props.consAnsPreview.previewRowCount}/
                {this.props.consAnsPreview.totalRowCount} rows,{" "}
                {this.props.consAnsPreview.totalEvaluationTime} ms)
              </p>
            </div>
          </div>
          <div className="row">
            <div className="col-md-12">
              <BootstrapTable
                striped
                bordered={false}
                hover
                condensed
                bootstrap4={true}
                headerClasses="bg-secondary text-white"
                keyField={tableDetail.name + "-data"}
                key={tableDetail.name + "-data"}
                data={tableDetail.data}
                columns={tableDetail.columns}
              />
            </div>
          </div>
        </div>
      );
    }
  }
}

export default ConsistentAnswers;
