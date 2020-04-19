import React, { Component } from "react";
import "./index.css";
class FrontpageDescription extends Component {
  state = {};
  render() {
    return (
      <main role="main" className="container top-spacing">
        <h1 className="mt-5">CAvSAT</h1>
        <h5 className="mb-md-4">Consistent Answers via Satisfiability</h5>
        <p className="lead">
          CAvSAT is a system for answering queries over inconsistent databases
          w.r.t. repair semantics. This prototype UI is built using React, and
          connects to CAvSAT modules via REST API calls. Thank you for trying
          out CAvSAT!
        </p>
        <p className="mb-md-4">Start by connecting to a database below.</p>
      </main>
    );
  }
}

export default FrontpageDescription;
