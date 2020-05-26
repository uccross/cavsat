/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.bean;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Akhil
 *
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DBEnvironment {
	private String dbEngine;
	private String dbDisplayName;
	private String serverIPAddress;
	private String serverPort;
	private String username;
	private String password;
	private List<String> schemas;
}
