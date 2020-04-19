import React, { Component } from "react";

class Modal extends Component {
  render() {
    function tabActive(index) {
      if (index === 0) return " show active ";
      return "";
    }
    const tabHeaders = this.props.tabHeaders.map((tabHeader, index) => (
      <a
        className={"nav-item nav-link btn-outline-secondary mr-1".concat(
          tabActive(index)
        )}
        id={"nav-modal-tab".concat(index)}
        data-toggle="tab"
        href={"#modal-tab".concat(index, this.props.modalId)}
        role="tab"
        aria-controls="nav-home"
        aria-selected="true"
      >
        {tabHeader}
      </a>
    ));

    const tabBodies = this.props.tabBodies.map((tabBody, index) => (
      <div
        className={"tab-pane fade".concat(tabActive(index))}
        id={"modal-tab".concat(index, this.props.modalId)}
        role="tabpanel"
        aria-labelledby={"modal-tab".concat(index)}
      >
        <pre>
          <code className="sql">{tabBody}</code>
        </pre>
      </div>
    ));

    return (
      <div
        className="modal fade"
        id={this.props.modalId}
        tabIndex="-1"
        role="dialog"
        aria-labelledby={"ModalLabel".concat(this.props.modalId)}
        aria-hidden="true"
      >
        <div className="modal-dialog modal-lg" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <h5 className="modal-title">{this.props.modalHeader}</h5>
              <div className="nav nav-pills" role="tablist">
                {tabHeaders}
              </div>
            </div>
            <div className="modal-body" id="modal-body">
              <div
                className="tab-content py-0 px-3 px-sm-0"
                id="nav-tabContent"
              >
                {tabBodies}
              </div>
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
export default Modal;
