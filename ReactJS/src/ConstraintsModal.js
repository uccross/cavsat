import React, { Component } from "react";

class ConstraintsModal extends Component {
  render() {
    if (this.props.constraints === null || this.props.constraints === undefined)
      return <div></div>;
    return (
      <div
        className="modal fade"
        id="constraintsModal"
        tabIndex="-1"
        role="dialog"
        aria-labelledby="constraintsModalLabel"
        aria-hidden="true"
      >
        <div className="modal-dialog modal-lg" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <h5 className="modal-title" id="constraintsModalLabel">
                Integrity Constraints on{" "}
                <span className="text-info">{this.props.schemaName}</span>
              </h5>
              <button
                type="button"
                className="close"
                data-dismiss="modal"
                aria-label="Close"
              >
                <span aria-hidden="true">&times;</span>
              </button>
            </div>
            <div className="modal-body">
              <table className="table table-bordered table-sm">
                <thead>
                  <tr>
                    <th scope="col">#</th>
                    <th scope="col">Constraint Type</th>
                    <th scope="col">Constraint Definition</th>
                  </tr>
                </thead>
                <tbody>
                  {this.props.constraints.map(constraint => (
                    <tr key={constraint.constraintId}>
                      <td>{constraint.constraintId}</td>
                      <td>
                        <code>{constraint.constraintType}</code>
                      </td>
                      <td>
                        <code>{constraint.constraintDefinition}</code>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="modal-footer">
              <button
                type="button"
                className="btn btn-secondary"
                data-dismiss="modal"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default ConstraintsModal;
