import React, { Component } from "react";
import DBConnectedHeader from "./DBConnectedHeader";
import "./index.css";

class TopNavbar extends Component {
  constructor(props) {
    super(props);
    const schemaName = localStorage.getItem("schemaName");
    this.props.fetchDatabasePreview(schemaName);
    this.props.fetchCAvSATConstraints(schemaName);
  }
  render() {
    var DBConnected;
    if (this.props.dbEnv === null || this.props.dbEnv === undefined) {
      DBConnected = <div></div>;
    } else {
      DBConnected = (
        <DBConnectedHeader
          handleDBConnected={this.props.handleDBConnected.bind(this)}
          fetchDatabasePreview={this.props.fetchDatabasePreview.bind(this)}
          fetchCAvSATConstraints={this.props.fetchCAvSATConstraints.bind(this)}
          dbEnv={this.props.dbEnv}
          constraints={this.props.constraints}
        />
      );
    }
    return (
      <nav className="navbar nav fixed-top navbar-expand navbar-dark bg-dark">
        <a className="navbar-brand" href="/">
          CAvSAT
        </a>
        <div className="collapse navbar-collapse">
          <ul className="navbar-nav mr-auto">
            <li className="nav-item">
              <a
                className="nav-link"
                target="_blank"
                rel="noopener noreferrer"
                href="https://github.com/uccross/cavsat/"
              >
                <i className="fab fa-github"></i>
              </a>
            </li>
          </ul>
          {DBConnected}
        </div>
      </nav>
    );
  }
}

export default TopNavbar;
