/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Akhil
 *
 */
@AllArgsConstructor
@Data
public class Node {
	private String id;
	private int group;
}
